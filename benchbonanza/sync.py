#!/usr/bin/env python3
from sys import argv
from pathlib import Path

from psycopg import connect as dbconnect

from benchbonanza.lib.gitstate import GitState
from benchbonanza.lib.util import chat


def git_syncstate_to_db(conn, syncstate):
    cur = conn.cursor()
    for moniker, (is_success, timestamp, *err) in syncstate.items():
        if is_success is None:
            cur.execute(
                """
                INSERT INTO repo
                    (moniker, last_successful_sync_at, last_failed_sync_at)
                VALUES
                    (%s, NULL, NULL)
                ON CONFLICT DO NOTHING;
                """,
                (moniker,)
            )
        else:
            state_column = {True: 'last_successful_sync_at', False: 'last_failed_sync_at'}[is_success]
            cur.execute(
                f"""
                INSERT INTO repo
                    (moniker, {state_column})
                VALUES
                    (%(moniker)s, %(timestamp)s)
                ON CONFLICT (moniker) DO UPDATE SET {state_column} = %(timestamp)s;
                """,
                dict(moniker=moniker, timestamp=timestamp)
            )


def commits_to_db(conn, commits):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TEMPORARY TABLE gitcommit_fresh (
            commithash binsha1 PRIMARY KEY,
            ts timestamptz NOT NULL
        ) ON COMMIT DROP;
        """
    )
    for commit, ts in commits.items():
        cur.execute(
            """
            INSERT INTO gitcommit_fresh (commithash, ts) VALUES (%s, to_timestamp(%s));
            """,
            (commit, ts)
        )
    cur.execute(
        """
        MERGE INTO gitcommit c
        USING gitcommit_fresh f
        ON (c.commithash = f.commithash)
        WHEN NOT MATCHED THEN
            INSERT VALUES (f.commithash, f.ts)
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE;
        DROP TABLE gitcommit_fresh;
        """
    )
    return len(commits)


def applications_to_db(conn, applications):

    def iter_applications():
        for repomoniker, apps in applications.items():
            for exp, commitset in apps.items():
                for commit in commitset:
                    yield (repomoniker, exp, commit)

    cur = conn.cursor()
    cur.execute(
        """
        CREATE TEMPORARY TABLE application_fresh (
            repo varchar(16) NOT NULL,
            epath_truncated ltree NOT NULL,
            commithash binsha1 NOT NULL
        ) ON COMMIT DROP;
        """
    )

    application_cnt = 0
    for row in iter_applications():
        cur.execute(
            """
            INSERT INTO application_fresh (repo, epath_truncated, commithash) VALUES (%s, %s, %s);
            """,
            row
        )
        application_cnt += 1
    cur.execute(
        """
        WITH af_expanded AS (
            SELECT DISTINCT e.epath, c.commithash
            FROM application_fresh af
            INNER JOIN repo ON (repo.moniker = af.repo)
            INNER JOIN gitcommit c ON (c.commithash = af.commithash)
            INNER JOIN experiment e ON (e.epath <@ af.epath_truncated)
        )
        MERGE INTO application a
            USING af_expanded af
            ON ((a.epath, a.commithash) = (af.epath, af.commithash))
            WHEN NOT MATCHED THEN
                INSERT (epath, commithash) VALUES (af.epath, af.commithash)
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE;
        """
    )
    cur.execute(
        """
        WITH af_expanded AS (
            SELECT DISTINCT repo.moniker as repo, e.epath, c.commithash
            FROM application_fresh af
            INNER JOIN repo ON (repo.moniker = af.repo)
            INNER JOIN gitcommit c ON (c.commithash = af.commithash)
            INNER JOIN experiment e ON (e.epath <@ af.epath_truncated)
        )
        MERGE INTO application_via_repo avr
            USING (
                SELECT app.id as application_id, af.repo
                FROM application app
                INNER JOIN af_expanded af ON (
                    (app.epath, app.commithash) = (af.epath, af.commithash)
                )
            ) avr_fresh
            ON ((avr.application_id, avr.repo) = (avr_fresh.application_id, avr_fresh.repo))
            WHEN NOT MATCHED THEN
                INSERT (application_id, repo) VALUES (avr_fresh.application_id, avr_fresh.repo)
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE;

        DROP TABLE application_fresh;
        """
    )
    return application_cnt


def tags_to_db(conn, tags):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TEMPORARY TABLE gitcommit_has_tag_fresh (
            commithash binsha1 NOT NULL,
            repo varchar(16) NOT NULL,
            tag text NOT NULL
        ) ON COMMIT DROP;
        """
    )

    tag_cnt = 0
    for repomoniker, tagspec in tags.items():
        for commit, tagset in tagspec.items():
            for tag in tagset:
                cur.execute(
                    """
                    INSERT INTO gitcommit_has_tag_fresh (commithash, repo, tag) VALUES (%s, %s, %s);
                    """,
                    (commit, repomoniker, tag)
                )
                tag_cnt += 1

    cur.execute(
        """
        MERGE INTO gitcommit_has_tag t
            USING gitcommit_has_tag_fresh tf
            ON ((t.commithash, t.repo, t.tag) = (tf.commithash, tf.repo, tf.tag))
            WHEN NOT MATCHED THEN
                INSERT VALUES (tf.commithash, tf.repo, tf.tag)
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE;

        DROP TABLE gitcommit_has_tag_fresh;
        """
    )
    return tag_cnt


def exp_definitions_to_db(conn, experiments):
    exp_etrees = []
    for (exp, variant, machines), _expfilepath in experiments:
        for machine in machines:
            exp_etrees.append(f'{exp}.{variant}.{machine}')
        cur = conn.cursor()
        cur.execute(
            """
            WITH fresh_defs AS (
                SELECT UNNEST(%s::ltree[]) AS epath
            )
            MERGE INTO experiment ex
            USING fresh_defs fresh
            ON (ex.epath = fresh.epath)
            WHEN NOT MATCHED THEN
                INSERT VALUES (fresh.epath)
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE;
            """,
            (exp_etrees,)
        )
    return len(exp_etrees)


def syncup(statedir):
    gitstate = GitState(statedir)
    syncstate = gitstate.sync()
    harvest_cnts = {'Tracked repos': len(syncstate) -1 }
    with dbconnect() as conn:
        git_syncstate_to_db(conn, syncstate)
        harvest_cnts['Experiments'] = exp_definitions_to_db(conn, gitstate.get_experiments(with_machines=True))

        harvest = gitstate.get_state()
        with conn.transaction():
            harvest_cnts['Commits'] = commits_to_db(conn, harvest['commits'])
            harvest_cnts['Applications'] = applications_to_db(conn, harvest['applications'])
            harvest_cnts['Tags'] = tags_to_db(conn, harvest['repocommit_tags'])

    for catcnt in harvest_cnts.items():
        chat('{:…<20} {}'.format(*catcnt))

def main():
    usage = f"Usage:\n\t{argv[0]} STATEDIR\nAlso needs:\n\t— PG* database connection environment variables set\n\t— CONFIGREPO_GIT_URL environment variable set\n"
    try:
        statedir, *rest = argv[1:]
    except ValueError:
        exit(usage)
    statedir = Path(statedir)
    if not statedir.is_dir():
        exit(f'Not a directory: {statedir}')
    try:
        syncup(statedir)
    except KeyboardInterrupt:
        exit('Interrupted')


if __name__ == '__main__':
    main()
