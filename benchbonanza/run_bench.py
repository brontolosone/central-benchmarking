#!/usr/bin/env python3
from subprocess import run, DEVNULL
from os import environ

from psycopg import connect as dbconnect



def find_work():
    with dbconnect() as dbconn:
        with dbconn.cursor() as cur:
            cur.execute(
                """
                WITH application_via_any_repo AS (
                    SELECT DISTINCT ON (application_id) *
                    FROM application_via_repo
                )
                SELECT avar.repo, app.epath, encode(app.commithash, 'hex') as commithex
                FROM application app
                    LEFT OUTER JOIN application_run ar ON (app.id = ar.application_id)
                    INNER JOIN application_via_any_repo avar ON (app.id = avar.application_id)
                WHERE ar.application_id IS NULL
                LIMIT 1
                """
            )
            return next(cur, None)


def invoke(testargv):
    argv = ['bb-bench', environ['STATE_DIRECTORY'], *testargv]
    ran = run(argv, stdin=DEVNULL, stdout=None, stderr=None)
    return ran.returncode


def main():
    if work := find_work():
        returncode = invoke(work)
        with dbconnect() as dbconn:
            with dbconn.transaction():
                dbconn.execute(
                    """
                    UPDATE application_run
                    SET
                        ended_at = now(),
                        exitcode = %s
                    WHERE
                        instance = %s
                    """,
                    (returncode, environ['INVOCATION_ID'])
                )
        exit(returncode)


if __name__ == '__main__':
    main()
