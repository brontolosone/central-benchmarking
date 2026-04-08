#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor, as_completed
from subprocess import Popen, run, check_output, DEVNULL
from tempfile import mkdtemp
from os import getpid
from os.path import join as pathjoin, basename as pathbasename
from pathlib import Path
from itertools import starmap, product, chain
from time import sleep, monotonic
from sys import stdout, argv
from shutil import rmtree
from json import dumps as jsondumps
import atexit

from varlink import Client
from varlink.error import InvalidParameter, VarlinkError


VARLINK_STAT_KEYS = {
    "IPIngressBytes", "IPIngressPackets", "IPEgressBytes", "IPEgressPackets",
    "IOReadBytes", "IOReadOperations", "IOWriteBytes", "IOWriteOperations",
    "TasksCurrent",
}
CGROUP_FILES = 'cpu.pressure io.pressure memory.pressure cpu.stat memory.stat memory.peak'.split()
CGROUP_DIR = '/sys/fs/cgroup/system.slice'


class StatReader:

    def __init__(self, cid, ssh_configfile, varlink_port, services):
        self.services = services
        self.sockdir = mkdtemp(prefix=f'benchbonanza-statreader-sockets-{getpid()}_')

        self.varlink_socket = pathjoin(self.sockdir, 'varlink')
        self.varlink_proxy = self.get_varlink_proxy(cid, varlink_port)
        self.varlinkconns = {service: Client(f'unix:{self.varlink_socket}').open('io.systemd.Unit') for service in self.services}

        self.cgroup_dirs = {service: pathjoin(CGROUP_DIR, service) for service in self.services}
        self.ssh_configfile = ssh_configfile
        self.pool = ThreadPoolExecutor(max_workers=1 + len(self.services), thread_name_prefix='statreader-')
        self.shutdown = False


    def get_varlink_proxy(self, cid, varlink_port):
        proxy = Popen(
            ['/usr/bin/socat', f'UNIX-LISTEN:{self.varlink_socket},reuseaddr,fork', f'VSOCK-CONNECT:{cid}:{varlink_port}'],
            stdin=DEVNULL,
            stdout=DEVNULL,
            stderr=DEVNULL,
            env={},
        )
        # wait for socket to appear
        p_varlink_socket = Path(self.varlink_socket)
        while not p_varlink_socket.is_socket():
            sleep(0.01)
        return proxy


    def get_varlink_stats(self, unit):
        varlinkconn = self.varlinkconns[unit]
        try:
            status = varlinkconn.List(name=unit)['runtime']['CGroup']
            return tuple(f'{unit}/varlink:{k} {v}' for k, v in status.items() if k in VARLINK_STAT_KEYS)  # matches format of what we get out of the cgstats
        except InvalidParameter as err:
            if err.parameters().get('parameter') == 'name':  # Unit not (or not *yet*) known
                pass
            else:
                raise err
        except VarlinkError as err:
            if err.error() == 'io.systemd.Unit.NoSuchUnit':  # Unit disappeared. Oh well.
                pass
            else:
                raise err


    def get_cgroup_stats(self):
        cgroup_files = list(starmap(pathjoin, product(self.cgroup_dirs.values(), CGROUP_FILES)))
        extract_argv = ['/bin/busybox', 'fgrep', '-H', '-F', ''] + cgroup_files
        popen_argv = ['/usr/bin/ssh', '-F', str(self.ssh_configfile), 'benchee', jsondumps(extract_argv)]  # extract_argv is passed on to arghsh over ssh
        measurement_slice = slice(len(CGROUP_DIR) + 1, None)
        return tuple(line[measurement_slice] for line in filter(None, run(popen_argv, capture_output=True, encoding='utf-8').stdout.splitlines()))


    def get_stats(self):
        futures = []
        for sv in self.services:
            futures.append(self.pool.submit(self.get_varlink_stats, sv))
        futures.append(self.pool.submit(self.get_cgroup_stats))
        return tuple(filter(None, (f.result() for f in as_completed(futures))))


    def run(self, interval: int):
        if self.shutdown:
            raise RuntimeError("Terminated StatReader cannot run again")
        # get starttime of remote system; to align measurements on
        starttime = check_output(['/usr/bin/ssh', '-F', str(self.ssh_configfile), 'benchee', jsondumps(['/usr/local/bin/monotonic'])], stdin=DEVNULL, stderr=DEVNULL, encoding='utf-8')
        starttime = float(starttime)  # to raise if it's not a float
        print(starttime, flush=True)
        seq = 0
        while not self.shutdown:
            sleep(interval - divmod(monotonic(), interval)[1])
            stats = self.get_stats()
            seq += interval
            for line in chain.from_iterable(stats):
                print(seq, '\t', line, sep='')
            stdout.flush()
        self.close()


    def close(self):
        if self.shutdown:
            return
        self.shutdown = True
        self.pool.shutdown()
        for vlc in self.varlinkconns.values():
            vlc.close()
        self.varlink_proxy.terminate()
        rmtree(self.sockdir, ignore_errors=True)


def main():
    usage = f"Usage: {pathbasename(argv[0])} interval cid ssh_configfile varlink_port *services"
    try:
        interval, cid, ssh_configfile, varlink_port, *services = argv[1:]
        interval, cid, varlink_port = map(int, (interval, cid, varlink_port))
        ssh_configfile = Path(ssh_configfile)
        if not ssh_configfile.is_file:
            exit(f'Not a file: {ssh_configfile}')
    except ValueError:
        exit(usage)
    statter = StatReader(
        cid,
        ssh_configfile,
        varlink_port,
        services
    )
    atexit.register(statter.close)
    try:
        statter.run(interval)
    except KeyboardInterrupt:
        exit("Interrupted")
    except BrokenPipeError:
        exit(2)


if __name__ == '__main__':
    main()
