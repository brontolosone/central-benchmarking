#!/usr/bin/env python3.14
from signal import signal, SIGALRM, alarm
import socket
from tempfile import gettempdir
from pathlib import Path
from os import environ
from uuid import uuid7
from subprocess import run, check_call, Popen, CalledProcessError, TimeoutExpired, DEVNULL
from struct import unpack
from resource import getpagesize
from time import monotonic, sleep
from shutil import rmtree
from atexit import register as atexit_register
from json import dumps as jsondumps, loads as jsonloads, JSONDecodeError
from shlex import split as shlexsplit
from functools import partial
from itertools import chain

from .util import popen_check_running

PAGESIZE = getpagesize()
check_call_hushed = partial(check_call, stdout=DEVNULL, stderr=DEVNULL)

class QemuException(Exception):
    pass

class VMDidNotBoot(QemuException):
    pass

class VMInvocationException(QemuException):
    pass

class VMTimeoutExpired(QemuException):
    def __init__(self, timeout):
        self.message=f'VM timeout expired: {timeout}s', 
        self.timeout=timeout

def to_argv(cmd):
    if isinstance(cmd, str):
        cmd = shlexsplit(cmd)
    return jsondumps(list(map(str, cmd)))


class GuestVM:
    QEMU_EARLY_TERMINATION_CHECK_SECONDS = 0.5
    QEMU_SHUTDOWN_KILL_TIMEOUT = 10  # kill qemu when it doesn't honor the shutdown request
    SSHD_UP_CHECK_TIMEOUT = 15
    SSHD_VSOCK_PORT = 22
    MONITOR_SOCKET_FILENAME = 'qemu-monitor.sock'
    QMP_SOCKET_FILENAME = 'qemu-qmp.sock'
    DISK_IMAGE_FILENAME = 'disk.img'
    GUEST_CONSOLE_SOCKET_FILENAME = 'qemu-console.sock'
    SSH_KEYDIR = 'ssh'

    def __init__(
        self,
        cpucnt: int = 1,
        memsize: int = 256,  # in megabytes
        kernel_bzImage: str = None,
        virtiofsd_socket: str = None,
        machine_id = None,
        hugepagesize = 0,  # 0 = no hugepages
        disk_img_location = gettempdir(),
        disk_size: int = 4,  # in gigabytes
        disk_throttle = None,
        qemu_stdout = None,
        qemu_stderr = None,
        timeout = None,
        extra_credentials = None,
        ssh_for_group = True,
    ):
        if not all((kernel_bzImage, virtiofsd_socket)):
            raise ValueError('kernel_bzImage and virtiofsd_socket are required')
        self.closed = False
        self.cleaned = False
        atexit_register(lambda: self.close())
        self.cpucnt = cpucnt
        self.memsize = memsize
        self.kernel_bzImage = kernel_bzImage

        self.timeout = timeout
        self.extra_credentials = extra_credentials or {}
        self.machine_id = machine_id or uuid7()
        self.vsock_cid = unpack('<I', self.machine_id.bytes[:4])[0]
        self.virtiofsd_socket = virtiofsd_socket
        self.disk_img = Path(disk_img_location, self.DISK_IMAGE_FILENAME)
        self.hugepagesize = hugepagesize
        self.tempdir = Path(environ.get('RUNTIME_DIRECTORY', Path(gettempdir(), 'benchbonanza')), str(self.machine_id))
        self.tempdir.mkdir(parents=True)
        self.ssh_keyfile = self.tempdir / self.SSH_KEYDIR / 'id_ed25519'
        self.ssh_config = self.tempdir / 'ssh_config'
        self.qemu_stdout = (self.tempdir / 'qemu_stdout') if qemu_stdout is None else qemu_stdout
        self.qemu_stderr = (self.tempdir / 'qemu_stderr') if qemu_stderr is None else qemu_stderr
        self.sshkey_mirror_process = None
        self.qemu = self.launch_qemu(disk_size=disk_size, disk_throttle=disk_throttle, ssh_for_group=ssh_for_group)
        self.wait_rexec_socket(self.SSHD_UP_CHECK_TIMEOUT)

    def __repr__(self):
        return f'GuestVM @ {self.vsock_cid} / {self.machine_id}'

    @property
    def monitor_socket(self):
        return self.tempdir / self.MONITOR_SOCKET_FILENAME

    @property
    def qmp_socket(self):
        return self.tempdir / self.QMP_SOCKET_FILENAME

    @property
    def console_socket(self):
        return self.tempdir / self.GUEST_CONSOLE_SOCKET_FILENAME


    def wait_rexec_socket(self, timeout=15):
        with socket.socket(socket.AF_VSOCK) as sock:
            deadline = monotonic() + timeout
            while True:
                try:
                    sock.connect((self.vsock_cid, self.SSHD_VSOCK_PORT))
                    break
                except OSError as nope:
                    if monotonic() > deadline:
                        self.shutdown()
                        raise VMDidNotBoot
                    if nope.errno == 19:
                        sleep(0.05)
                except ConnectionResetError:
                    # too early?
                    sleep(0.05)

    def qemu_cmd(self):
        pubkey = Path(str(self.ssh_keyfile) + '.pub').read_text()
        maybe_hugepages = '' if not self.hugepagesize else f',hugetlb=on,hugetlbsize={self.hugepagesize}'

        # useful kernel args:
        # debug
        # systemd.journald.forward_to_console=true
        kernel_args = f"""
            console=hvc0
            root=root rootfstype=virtiofs rw
            init=/usr/lib/systemd/systemd
            systemd.log_color=false
            systemd.ssh_auto=on
            systemd.firstboot=off
            systemd.machine_id={self.machine_id.hex}
            systemd.hostname=benchbonanza-{self.vsock_cid}
            systemd.unit=multi-user.target
        """

        smbios_extra_credentials = list(
            chain.from_iterable(
                (
                    ('-smbios', f'type=11,value=io.systemd.credential:{k}={v}')
                    for k,v in self.extra_credentials.items()
                )
            )
        )

        return [
            '/usr/bin/qemu-system-x86_64',
            '-smbios', f'type=11,value=io.systemd.credential:ssh.authorized_keys.root={pubkey}',
            *smbios_extra_credentials,
            '-sandbox', 'obsolete=on,elevateprivileges=on,spawn=on,resourcecontrol=on',
            '-no-user-config',

            '-cpu', 'host',
            '-enable-kvm',
            '-machine', 'q35,accel=kvm,mem-merge=off',

            '-name', f'benchbonanza-{self.machine_id}',
            '-uuid', str(self.machine_id),

            '-kernel', self.kernel_bzImage,
            '-append', ' '.join((line.strip() for line in kernel_args.splitlines())),

            '-device', 'virtio-serial-pci',

            # for console in invoking terminal
            '-device', 'virtconsole,chardev=gassie-console-stdio',
            '-chardev', 'stdio,signal=off,id=gassie-console-stdio',

            # for serial console
            '-device', 'virtconsole,chardev=gassie-console-serial',
            '-chardev', f'socket,id=gassie-console-serial,path={self.console_socket},server=on,wait=off',

            '-monitor', f'unix:{self.monitor_socket},server=on,wait=off',
            '-qmp', f'unix:{self.qmp_socket},server=on,wait=off',
            '-display', 'none',
            '-vga', 'none',

            '-chardev', f'socket,id=virtiofsd-char,path={self.virtiofsd_socket}',
            '-device', 'vhost-user-fs-pci,queue-size=1024,chardev=virtiofsd-char,tag=root',

            '-object', f'memory-backend-memfd,id=mem0,merge=off,dump=off,share=on,prealloc=on,size={self.memsize}M,seal=on{maybe_hugepages}',
            '-smp', f'{self.cpucnt},sockets={self.cpucnt},maxcpus={self.cpucnt}',
            '-m', f'{self.memsize}M',
            '-numa', f'node,nodeid=0,cpus=0-{self.cpucnt - 1},memdev=mem0',
            '-overcommit', 'mem-lock=on',

            '-device', f'vhost-vsock-pci,id=vhost-vsock-pci0,guest-cid={self.vsock_cid}',

            '-blockdev', f'driver=file,node-name=disk-blockdev,filename={self.disk_img},aio=io_uring,cache.direct=on,discard=ignore,detect-zeroes=off',
            '-device', 'virtio-blk-pci,id=disk-drive,discard=off,drive=disk-blockdev',
        ]

    def get_ssh_config(self):
        return f"""
        Host *
            ProxyCommand /usr/lib/systemd/systemd-ssh-proxy vsock%%{self.vsock_cid} {self.SSHD_VSOCK_PORT}
            ProxyUseFdpass yes
            StrictHostKeyChecking no
            UserKnownHostsFile /dev/null
            CheckHostIP no
            UpdateHostKeys no

            IdentityFile {self.ssh_keyfile}
            IdentitiesOnly yes
            ForwardAgent no

            Compression no
            LogLevel ERROR
            ServerAliveInterval 2

        Host benchee
            ControlMaster auto
            ControlPath {self.tempdir}/sshmux-%r
            ControlPersist 30
            User arghshroot

        Host vm
            User root
        """

    def ssh_argv(self):
        return [
            '/usr/bin/ssh',
            '-T',  # no PTY
            '-F', str(self.ssh_config),
            'benchee',
        ]

    def rexec_argv(self, cmd):
        return self.ssh_argv() + [to_argv(cmd)]

    def rexec_run(self, cmd, **kwargs):
        return run(
            self.rexec_argv(cmd),
            **{'env': {}, **kwargs}
        )

    def rexec_popen(self, cmd, **kwargs):
        return Popen(
            self.rexec_argv(cmd),
            **{'env': {}, **kwargs}
        )

    def launch_qemu(self, disk_size=4, disk_throttle=None, ssh_for_group=True):
        # register a cleanup hook should we abort prematurely
        atexit_register(self.cleanup, missing_ok=True)
        # create disk image
        check_call_hushed(['/usr/bin/btrfs', 'filesystem', 'mkswapfile', '--size', f'{disk_size}G', f'{self.disk_img}'])
        check_call_hushed(['/usr/bin/mkfs.ext4', '-E', 'assume_storage_prezeroed=1,num_backup_sb=0,root_perms=777', '-m', '0', '-F', '-L', 'disk', '-U', f'{self.machine_id}', f'{self.disk_img}'])
        # create an SSH keypair
        ssh_keyfile_dir = self.ssh_keyfile.parent
        ssh_keyfile_dir.mkdir()
        check_call_hushed(['/usr/bin/ssh-keygen', '-q', '-t', 'ed25519', '-P', '', '-C', f'benchbonanza-{self.machine_id}', '-f', f'{self.ssh_keyfile}'])
        # SSH disallows using a group-readable private key, unless the owner is not the invoking user.
        # We can't chown ownership away (without more privileges), but we can fake it through bindfs.
        if ssh_for_group:
            proc = Popen(
                [
                    'bindfs',
                    '--force-user=nobody',
                    '--perms=u=,g=rX,o=',
                    *[f'--{thing}-deny' for thing in ('chown', 'chgrp', 'chmod', 'delete', 'rename')],
                    '--xattr-none',
                    '--no-allow-other',
                    '-f',  # foreground, fuse option
                    '-o', 'ro',  # readonly, fuse option
                    str(ssh_keyfile_dir), str(ssh_keyfile_dir),  # overmount
                ],
                stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL,
            )
            popen_check_running(proc)
            self.sshkey_mirror_process = proc
        # create the SSH config
        with open(self.ssh_config, 'w') as thefile:
            thefile.write(self.get_ssh_config())
        # finally, the QEMU process
        qemuproc = Popen(self.qemu_cmd(), close_fds=True, stdin=DEVNULL, stdout=self.qemu_stdout.open('xb', buffering=0), stderr=self.qemu_stderr.open('xb'))
        # check for early return — invocation/startup errors etc
        try:
            popen_check_running(qemuproc, timeout=self.QEMU_EARLY_TERMINATION_CHECK_SECONDS)
        except CalledProcessError as err:
            raise VMInvocationException from err

        if self.timeout:
            signal(SIGALRM, self.on_timeout)
            alarm(self.timeout)

        if disk_throttle:
            sanitized = {}
            for cat in ('iops', 'bps'):
                if combined := disk_throttle.get(cat):
                    sanitized[cat] = combined
                else:
                    for key in (f'{cat}_rd', f'{cat}_wr'):
                        sanitized[key] = disk_throttle.get(key, 0)
            self.qmp_chat({
                "execute": "block_set_io_throttle",
                "arguments": {
                    "id": "disk-drive/virtio-backend",
                    **sanitized,
                }
            })

        return qemuproc

    def on_timeout(self, _signum, _frame):
        raise VMTimeoutExpired(self.timeout)

    def cleanup(self, **kwargs):
        if self.cleaned:
            return
        if self.sshkey_mirror_process:
            self.sshkey_mirror_process.terminate()
        self.disk_img.unlink(**kwargs)
        rmtree(self.tempdir, ignore_errors=True)
        self.cleaned = True


    def qmp_chat(self, msg: dict):
        PREAMBLE = jsondumps({"execute": "qmp_capabilities", "arguments": {"enable": ["oob"]}}).encode('utf-8')

        def chat(thesocket, message):
            if message is not None:
                thesocket.send(message + b'\r\n')
            chomped = b''
            # TODO: Not the prettiest way of synchronously processing the reply
            while chomp := sock.recv(PAGESIZE):
                chomped += chomp
                try:
                    return jsonloads(chomped)
                except JSONDecodeError:
                    pass  # Probably incomplete, block-wait for more data

        with socket.socket(socket.AF_UNIX) as sock:
            sock.connect(str(self.qmp_socket))
            _banner = chat(sock, None)
            _init = chat(sock, PREAMBLE)
            return chat(sock, jsondumps(msg).encode('utf-8'))


    def shutdown(self):
        with socket.socket(socket.AF_UNIX) as sock:
            try:
                sock.connect(str(self.monitor_socket))
                sock.send(b'quit\n')
                for _ in iter(lambda: sock.recv(PAGESIZE), b''):
                    pass  # happens to work as closing qemu closes the socket on the qemu-side, resulting in a 0-byte read. IOW this hangs if qemu hangs :-/
            except (FileNotFoundError, ConnectionResetError):
                pass
        try:
            self.qemu.wait(timeout=self.QEMU_SHUTDOWN_KILL_TIMEOUT)
        except TimeoutExpired:
            self.qemu.kill()
        except AttributeError:
            pass  # no qemu yet
        self.cleanup(missing_ok=True)


    def close(self):
        # For use with contextlib.closing
        if not self.closed:  # We may already have been cleaned up in a clean shutdown
            self.shutdown()
            self.closed = True
