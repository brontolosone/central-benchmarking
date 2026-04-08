#!/usr/bin/env python3
from binascii import unhexlify
from time import sleep, monotonic
from contextlib import closing
from pathlib import Path
from subprocess import DEVNULL, Popen, check_output, check_call
from sys import argv
from uuid import UUID, uuid7
from signal import signal, raise_signal, SIGUSR1
from threading import Thread
from json import loads as jsonloads
from os import environ, sendfile
from tomllib import load as tomlload

from psycopg import connect as dbconnect
from psycopg.errors import UniqueViolation, ForeignKeyViolation

from benchbonanza.lib import constants as const
from benchbonanza.lib.constants import Exitcode
from benchbonanza.lib.qemu import GuestVM, VMTimeoutExpired
from benchbonanza.lib.gitstate import GitState
from benchbonanza.lib.pgdisconnect import OnDBDisconnect
from benchbonanza.lib.process_perflog import read_log as read_perflog
from benchbonanza.lib.util import chat, db_read_one, popen_check_running
from benchbonanza.lib.benchparser.hey_summary_v1 import extract as hey_extract
from benchbonanza.lib.sparseify import sparseify

CONF_FILE = 'vm.toml'
STARTTIME = monotonic()

CONNECT_TMPL = """

    To connect to the VM console:

    minicom -D 'unix#{console_socket}'

    To SSH into the VM:

    ssh -F {ssh_config} vm

"""


def tstamp():
    return round(monotonic() - STARTTIME, 3)


def tchat(*args, **kwargs):
    chat(tstamp(), *args, **kwargs)


class VMCalledProcessError(Exception):
    pass


class Benchmark:
    VARLINK_VSOCK_PORT = 44
    PERFLOG = "perf.log"
    QEMU_STDOUT = "qemu_stdout.log"
    QEMU_STDERR = "qemu_stderr.log"
    JOURNAL_BIN = "journal.journal"  # systemd-journald-remote demands that this ends with .journal (for splitmode=none)
    JOURNAL_TXT = "journal.log"
    CHECKOUT_DIR = "/code"

    def __init__(self, statedir, repomoniker, epath, commithash):
        self.closed, self.stopped = False, False
        self.stderr = None if environ.get('BB_VERBOSITY') == '1' else DEVNULL
        signal(SIGUSR1, self.handle_SIGUSR1)
        self.conf = self.load_conf()
        self.statedir = statedir
        self.repomoniker = repomoniker
        self.epath = epath
        self.commithash = commithash
        self.experiment, self.variant, self.machine = self.epath.split('.', maxsplit=2)
        if self.machine not in self.conf['vm_available']:
            chat(f'Fatal: Unknown machine: {self.machine}')
            exit(Exitcode.UNKNOWN_MACHINE.value)
        self.instance = UUID(environ.get('INVOCATION_ID') or uuid7().hex)
        tchat(f'Booting VM {self.instance}')
        self.rundir = self.make_rundir(self.instance)
        self.gitstate = GitState(self.statedir)
        self.expconf = self.gitstate.get_expconf(self.experiment, self.variant)
        self.repoconf = self.gitstate.get_repoconf()[self.repomoniker]
        self.dbconn_disconnectmon = dbconnect(autocommit=False)
        self.dbdisconnect = OnDBDisconnect(self.on_db_conn_lost, self.dbconn_disconnectmon)
        self.application_id = self.dbregister()
        self.plaintext_journaler = None
        self.journaler, self.journal_remote_vsock_port = self.init_journaler()
        self.vm = self.init_vm()
        self.perflogger = None


    def make_rundir(self, instance):
        rundir = Path(self.statedir, const.EXPRUN_DIR, instance.hex[-2:], instance.hex)
        if not rundir.is_dir():
            rundir.mkdir(parents=True)
            check_call(['chattr', '+C', str(rundir)], stdin=DEVNULL, stdout=DEVNULL, stderr=self.stderr)
        return rundir


    def close(self):
        # for use with contextlib.closing
        tchat('Closing benchmark: cleanup')
        if not self.closed:
            tchat('Closing benchmark: cleanup: dbdisconnectmonitor')
            try:
                self.dbdisconnect.unmonitor()
            except AttributeError:
                pass
            tchat('Closing benchmark: cleanup: dbdisconnect DB conn')
            try:
                self.dbconn_disconnectmon.close()
            except AttributeError:
                pass
            self.stop()
            self.closed = True
        tchat('Closing benchmark: cleanup: done')


    def archive_rundir(self):
        # While appending to these log files, we wisely had nocow flags set, which precludes FS compression.
        # Now we turn each file into an FS-compressed and sparse version.
        stdstreamconfig = dict(stdin=DEVNULL, stdout=DEVNULL, stderr=self.stderr)
        for p in list(self.rundir.iterdir()):
            if p.is_file():
                p_new = Path(str(p) + '.tmp')
                with p_new.open('xb') as new:
                    check_call(['chattr', '-C', str(p_new)], **stdstreamconfig)
                    check_call(['btrfs', 'property', 'set', str(p_new), 'compression', 'zstd:15'], **stdstreamconfig)
                    with p.open('rb') as old:
                        sendfile(new.fileno(), old.fileno(), 0, p.stat().st_size)
                p_new.rename(p)
                sparseify(str(p))


    def stop(self):
        if not self.stopped:
            try:
                tchat('Closing benchmark: cleanup: journalers')
                self.journaler.terminate()
                self.plaintext_journaler.terminate()
            except AttributeError:
                pass
            try:
                tchat('Closing benchmark: cleanup: VM')
                self.vm.close()
            except AttributeError:
                pass
            tchat('Closing benchmark: cleanup: finalizing artifacts')
            self.archive_rundir()
            self.stopped = True


    def on_db_conn_lost(self):
        # Executed on the disconnect-watching thread, thus we use a signal to shift control back to the main thread
        raise_signal(SIGUSR1)


    def handle_SIGUSR1(self, signum, frame):
        tchat('Fatal: DB disconnect')
        self.close()
        exit(Exitcode.DB_DISCONNECT.value)


    def init_journaler(self):
        vsock_port = self.instance.int >> 96  # derive a 32-bit VSOCK port number
        journal_bin = self.rundir / self.JOURNAL_BIN
        if vsock_port < 1024:  # privileged range
            vsock_port += 1024
        logger_proc = Popen(
            [
                '/usr/lib/systemd/systemd-journal-remote',
                '--compress=no',
                '--seal=no',
                '--split-mode=none',
                f'--listen-raw=vsock:2:{vsock_port}',
                '--output', str(journal_bin),
            ],
            stdin=DEVNULL,
            stdout=DEVNULL,
            stderr=self.stderr,
        )
        popen_check_running(logger_proc)

        def launch_plaintext_journaler():
            # Keep up with the binary journal, outputting a text representation for easy inspection.
            # journalctl won't --follow on a journal file that hasn't received any
            # entry, so wait for it to become initialized.
            allballs8 = b'\x00\x00\x00\x00\x00\x00\x00\x00'
            with (journal_bin).open('rb', buffering=0) as binjournal:
                while binjournal.read(64)[56:] == allballs8:
                    binjournal.seek(0)
                    sleep(0.5)
            plaintext_logger_proc = Popen(
                ['/usr/bin/journalctl', '--boot', '--follow', '--no-tail', '--no-hostname', '--output=short-monotonic', '--quiet', '--no-pager', '--file', str(journal_bin)],
                stdin=DEVNULL,
                stdout=(self.rundir / self.JOURNAL_TXT).open('xb'),
                stderr=self.stderr,
            )
            popen_check_running(plaintext_logger_proc)
            self.plaintext_journaler = plaintext_logger_proc

        t = Thread(target=launch_plaintext_journaler, args=[], kwargs={}, daemon=True)
        t.start()
        return (logger_proc, vsock_port)


    def dbregister(self):
        with self.dbconn_disconnectmon.cursor() as cur:
            cur.execute(
                """
                SELECT pg_try_advisory_lock(hash_text_to_bigint(format('benchbonanza-exp-%%s@%%s', %s::text, %s::text)));
                """,
                (self.epath, self.commithash)
            )
            if not db_read_one(cur):
                tchat('Fatal: Cannot aquire advisory lock for ({self.epath=}, {self.commithash=})')
                exit(Exitcode.DB_RUN_LOCKED.value)
        try:
            with dbconnect() as dbconn:
                with dbconn.transaction():
                    with dbconn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO application_run
                                (application_id, instance, started_at)
                            (
                                SELECT id, %(instance)s, now() 
                                FROM application
                                WHERE epath = %(epath)s AND commithash = %(commithash)s
                            )
                            RETURNING
                                application_id
                            """,
                            dict(instance=self.instance, epath=self.epath, commithash=unhexlify(self.commithash))
                        )
                        application_id = db_read_one(cur)
                        if application_id is None:
                            tchat(f'Fatal: ({self.epath=}, {self.commithash=}) not present in table "experiment"')
                            exit(Exitcode.DB_RUN_APPLICATION_NONEXISTENT.value)
                        return application_id
        except UniqueViolation as err:
            tchat(f'Fatal: {err}')
            exit(Exitcode.DB_RUN_ALREADY_EXISTS.value)
        except ForeignKeyViolation as err:
            tchat(f'Fatal: {err})')
            exit(Exitcode.DB_RUN_INVALID_FOREIGN_KEY.value)


    def checkout_source(self):
        repodir = self.gitstate.repopath(self.repoconf)
        tarstream_producer = self.gitstate.export_repo(repodir, self.commithash)
        tarstream_consumer = self.vm.rexec_popen(
            ['/usr/bin/tar', 'x', '-f', '-', '-C', self.CHECKOUT_DIR],
            stdin=tarstream_producer.stdout,
            stderr=self.stderr,
            stdout=DEVNULL,
        )
        if any((tarstream_producer.wait(), tarstream_consumer.wait())):
            # one of these had a nonzero exit status
            raise VMCalledProcessError('git checkout')


    def cmditem2argv(self, cmditem, wait=True):

        def install_script(name, contents):
            if self.vm.rexec_run(['/usr/local/bin/enscript.sh', 'a+rx', f'/tmp/{name}'], encoding='utf-8', input=contents, stdout=DEVNULL, stderr=self.stderr).returncode:
                raise VMCalledProcessError(f'installing script: {name}')

        def envpair2assignment(pair):
            # pair is a dictionary with just 1 key
            for kv in pair.items():
                return '='.join(kv)

        script = None
        cmdname = cmditem['name']
        cmd = cmditem['cmd']
        argv = [
            '/usr/bin/systemd-run',
            '--unit', cmdname,
            '--service-type=exec',
            '--property', 'StandardOutput=journal',
            '--property', 'StandardError=journal',
        ]
        if wait:
            argv.append('--wait')
        if 'uid' in cmd:
            argv += ['--uid', cmd['uid']]
        if 'gid' in cmd:
            argv += ['--gid', cmd['gid']]
        if 'workdir' in cmd:
            argv += ['--working-directory', cmd['workdir']]
        for envpair in cmd.get('env', []):
            argv += ['--setenv', envpair2assignment(envpair)]
        exec = cmd['exec']
        if isinstance(exec, str):
            script = exec
            argv += ['--', f'/tmp/{cmdname}']
        else:
            argv += ['--'] + exec

        if script:
            install_script(cmdname, script)

        return cmdname, argv


    def run_cmd(self, cmditem, wait=True):
        cmdname, argv = self.cmditem2argv(cmditem, wait=wait)
        if self.vm.rexec_run(argv, stdin=DEVNULL, stdout=DEVNULL, stderr=self.stderr).returncode:
            raise VMCalledProcessError(cmdname)


    def get_output_of(self, servicename, transport='stdout'):
        return check_output(
            [
                'journalctl',
                '--file', str(self.rundir / self.JOURNAL_BIN),
                '--boot',
                '--output=cat',
                '--output-fields=MESSAGE',
                f'_SYSTEMD_UNIT={servicename}',
                f'_TRANSPORT={transport}',
            ],
            stdin=DEVNULL,
            stderr=self.stderr,
            encoding='utf-8',
        )


    def get_dwelltime_of(self, servicename):
        argv = ['journalctl', '--file', str(self.rundir / self.JOURNAL_BIN), '--boot', '--output=json', '--lines=1', f'UNIT={servicename}', '_EXE=/usr/lib/systemd/systemd']
        stds = dict(stdin=DEVNULL, stderr=self.stderr)
        start = jsonloads(check_output([*argv,
            'JOB_TYPE=start',
            'CODE_FUNC=job_emit_done_message',
        ], **stds))
        end = jsonloads(check_output([*argv,
            f'INVOCATION_ID={start['INVOCATION_ID']}',
            f'--after-cursor={start['__CURSOR']}',
            'CODE_FUNC=unit_log_success',
        ], **stds))
        return (int(start['__MONOTONIC_TIMESTAMP']), int(end['__MONOTONIC_TIMESTAMP']))


    def run_experiment(self):
        tchat('Checking out source code')
        self.checkout_source()

        # Run setup actions
        for cmditem in self.expconf.get('setup', []):
            tchat('Running setup:', cmditem['name'])
            self.run_cmd(cmditem)

        # Start logging performance
        def launch_perfmeasure(*services, sampling_interval=1):
            argv = ['bb-perflogger', sampling_interval, self.vm.vsock_cid, self.vm.ssh_config, self.VARLINK_VSOCK_PORT] + list(*services)
            perfproc = Popen(
                map(str, argv),
                stdin=DEVNULL,
                stdout=(self.rundir / self.PERFLOG).open('xb'),
                stderr=self.stderr,
            )
            popen_check_running(perfproc)
            return perfproc

        self.perflogger = launch_perfmeasure([f"{cmditem['name']}.service" for cmditem in self.expconf['trackedservice']])

        # Launch performance-tracked services
        for cmditem in self.expconf['trackedservice']:
            tchat('Launching trackedservice:', cmditem['name'])
            self.run_cmd(cmditem, wait=False)

        # Run the test load, recording finish times (for insertion of extracted stats on the timeline)
        for cmditem in self.expconf['testload']:
            tchat('Running testload:', cmditem['name'])
            self.run_cmd(cmditem)

        # Terminate the performance tracking, but if it crashed in the meantime, crash along
        popen_check_running(self.perflogger)
        self.perflogger.terminate()


    def get_metrics(self):
        exp_start_monotonic = None

        # most of the metrics, of the perflogger
        with (self.rundir / self.PERFLOG).open("rb") as raw_log:
            perf_tidbits = read_perflog(raw_log)
            exp_start_monotonic = next(perf_tidbits)
            yield from perf_tidbits

        # testload runtimes are metrics by themselves
        for cmditem in self.expconf['testload']:
            servicename = f'{cmditem['name']}.service'
            start, end = map(lambda num: round((num / 1_000_000) - exp_start_monotonic, ndigits=3), self.get_dwelltime_of(servicename))
            yield (start, servicename, 'start', 0)
            yield (end, servicename, 'end', int(round((end - start) * 1_000)))  # runtime in ms

            # metrics from testload output parsers
            if cmditem.get('stdout', {}).get('parser') == 'hey-summary-v1':
                for metric, number in hey_extract(self.get_output_of(servicename)):
                    yield (end, servicename, metric, number)


    def save_results(self):
        with dbconnect() as dbconn:
            with dbconn.transaction():
                with dbconn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TEMPORARY TABLE fresh_results
                            (at_t float4, mpath ltree, val bigint)
                        ON COMMIT DROP;
                        """
                    )
                    for at_t, servicename, metric, number in self.get_metrics():
                        cur.execute(
                            """
                            INSERT INTO fresh_results
                                (at_t, mpath, val)
                            VALUES
                                (%s, %s, %s)
                            """,
                            (at_t, '.'.join((servicename.split('.', maxsplit=1)[0], metric)), number),
                        )
                    # register metrics
                    cur.execute(
                        """
                        WITH fresh_metrics AS (
                            SELECT DISTINCT mpath as mpath FROM fresh_results ORDER BY mpath
                        )
                        MERGE INTO metric m
                        USING fresh_metrics fm
                        ON (m.mpath = fm.mpath)
                        WHEN NOT MATCHED THEN
                            INSERT (mpath)
                            VALUES (fm.mpath);
                        """
                    )
                    # finally, insert
                    cur.execute(
                        """
                        INSERT INTO measurement
                            (application_id, at_t, metric_id, val)
                        SELECT
                            %s, f.at_t, m.id, f.val
                        FROM
                            metric m INNER JOIN fresh_results f
                            ON
                            m.mpath = f.mpath
                        """,
                        (self.application_id,)
                    )


    def load_conf(self):
        with (Path(environ['CONFIGURATION_DIRECTORY']) / CONF_FILE).open('rb') as someconf:
            return tomlload(someconf)


    def init_vm(self):
        vm_timeout = min(self.expconf['meta'].get('timeout', const.VM_TIMEOUT_MAX), const.VM_TIMEOUT_MAX)
        vm_size = self.conf['vm_available'][self.machine]
        vm = GuestVM(
            **vm_size,
            **self.conf['vm'],
            disk_img_location = Path(environ['CACHE_DIRECTORY'], environ['INVOCATION_ID']),
            machine_id=self.instance,
            qemu_stdout = self.rundir / self.QEMU_STDOUT,
            qemu_stderr = self.rundir / self.QEMU_STDERR,
            timeout=vm_timeout,
            extra_credentials = {'journal.forward_to_socket': f'vsock:2:{self.journal_remote_vsock_port}'},
        )
        return vm


def main():
    try:
        statedir, repomoniker, epath, commithash = argv[1:]
    except ValueError:
        exit(f'Usage: {argv[0]} statedir repomoniker epath commithash')
    statedir = Path(statedir)
    if not statedir.is_dir():
        exit(f'Not a directory: {statedir}')
    try:
        with closing(Benchmark(statedir, repomoniker, epath, commithash)) as benchie:
            chat(CONNECT_TMPL.format(console_socket=benchie.vm.console_socket, ssh_config=benchie.vm.ssh_config))
            tchat('Running experiment')
            benchie.run_experiment()
            tchat('Terminating experiment')
            benchie.stop()
            tchat('Saving results')
            benchie.save_results()
    except KeyboardInterrupt:
        exit('\nInterrupted')
    except VMTimeoutExpired as err:
        tchat(f'Fatal: timeout ({err.timeout}) expired')
        exit(Exitcode.VM_TIMEOUT_EXPIRED.value)
    except VMCalledProcessError as err:
        tchat(f'Fatal: failure VM-executing step "{err.args[0]}"')
        exit(Exitcode.VM_PROCESS_ERROR.value)


if __name__ == "__main__":
    main()
