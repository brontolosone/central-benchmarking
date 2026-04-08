from sys import stderr
from functools import partial
from subprocess import TimeoutExpired, CalledProcessError
from os import environ


def mkchat():
    if environ.get('BB_VERBOSITY') == '1':
        return partial(print, file=stderr, flush=True)
    else:
        return lambda *args, **kwargs: None


chat = mkchat()


def db_read_one(cur):
    if mayberow := cur.fetchone():
        return next(iter(mayberow), None)


def popen_check_running(proc, timeout=0.5):
    # check for (early) termination
    try:
        if exitcode := proc.wait(timeout=timeout):
            # exitcode nonzero, that's not great
            raise CalledProcessError(exitcode, ' '.join(proc.args))
    except TimeoutExpired:
        pass  # Fine, means it's running!
