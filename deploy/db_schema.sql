CREATE DOMAIN binsha1 AS bytea CHECK (length(value) = 20);
CREATE DOMAIN ltreelabel AS varchar(255) CHECK (value ~ '^[a-zA-Z0-9_-]+$');
CREATE DOMAIN exptext AS text CHECK (value ~ '^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$');
CREATE DOMAIN sometext AS varchar(255) CHECK (length(value) > 0);
CREATE EXTENSION IF NOT EXISTS ltree;


CREATE FUNCTION hash_text_to_bigint(input text)
RETURNS bigint
AS
    $BODY$
        SELECT ('x' || md5(input))::bit(64)::bigint
    $BODY$
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE;


CREATE TABLE repo (
    moniker varchar(16) PRIMARY KEY NOT NULL CHECK (moniker ~ '^[a-zA-Z0-9_-]{1,16}$'),
    last_failed_sync_at timestamptz,
    last_successful_sync_at timestamptz
);
COMMENT ON TABLE repo IS 'Repos tracked';


CREATE TABLE gitcommit (
    commithash binsha1 PRIMARY KEY,
    ts timestamptz NOT NULL
);
CREATE INDEX ON gitcommit (ts DESC);
COMMENT ON TABLE gitcommit IS 'Commits as found (with a bb@-note for any known experiment) in the most recently scanned state of the configured repos';
COMMENT ON COLUMN gitcommit.ts IS 'Timestamp from git-log''s "committer date" (%ct format variable)';


CREATE TABLE gitcommit_has_tag (
    commithash binsha1 NOT NULL REFERENCES gitcommit (commithash) ON DELETE CASCADE,
    repo varchar(16) NOT NULL REFERENCES repo (moniker) ON DELETE CASCADE,
    tag text NOT NULL
);
CREATE UNIQUE INDEX ON gitcommit_has_tag USING btree (commithash, repo, tag text_pattern_ops);
COMMENT ON TABLE gitcommit_has_tag IS 'Tags found on experimentee-commits';


CREATE TABLE experiment (
    epath ltree NOT NULL PRIMARY KEY
);
CREATE INDEX ON experiment USING GIST (epath gist_ltree_ops(siglen=64));
COMMENT ON TABLE experiment IS 'Experiments as defined in configs; state here reflects the most recently scanned state of the experiment repo';
COMMENT ON COLUMN experiment.epath IS 'Experiment type designation: experiment.variant.machine, where machine = vendor.version.(vendor-specific, eg c(core count).m(memory in MB))';


CREATE TABLE application (
    id integer NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    epath ltree NOT NULL REFERENCES experiment (epath) ON DELETE CASCADE,
    commithash binsha1 NOT NULL REFERENCES gitcommit (commithash) ON DELETE CASCADE
);
CREATE UNIQUE INDEX ON application (epath, commithash);
COMMENT ON TABLE application IS 'Application of experiments to commits (normative)';


CREATE TABLE application_via_repo (
    application_id integer NOT NULL REFERENCES application (id) ON DELETE CASCADE,
    repo varchar(16) NOT NULL REFERENCES repo (moniker) ON DELETE CASCADE,
    PRIMARY KEY (application_id, repo)
);
COMMENT ON TABLE application_via_repo IS 'Tracks which repo(s) state that some experiment should be applied to some commit';


CREATE TABLE application_run (
    application_id integer PRIMARY KEY NOT NULL REFERENCES application (id) ON DELETE CASCADE,
    instance uuid UNIQUE NOT NULL,  -- set when experiment starts
    started_at timestamptz not null, -- set when experiment starts
    ended_at timestamptz,  -- set by experiment supervisor when experiment completes
    exitcode smallint  -- set by experiment supervisor when experiment completes
);
CREATE INDEX ON application_run (instance);
COMMENT ON TABLE application_run IS 'Experiment application outcome; created when an experiment starts, completed when it ends';
COMMENT ON COLUMN application_run.instance IS 'SystemD invocation ID/instance UUID of the VM created for this experiment';
COMMENT ON COLUMN application_run.started_at IS 'Start time of the experiment';
COMMENT ON COLUMN application_run.ended_at IS 'End time of the experiment (regardless of whether execution was successful)';
COMMENT ON COLUMN application_run.exitcode IS 'Process exit code — if non-zero, then there was some error';

CREATE VIEW experiment_status AS (
    SELECT
        a.id AS application_id,
        a.epath,
        a.commithash,
        ar.instance,
        ar.exitcode,
        pg_locks.pid AS locked_by_pid
    FROM
        application a
        LEFT OUTER JOIN application_run ar ON (a.id = ar.application_id)
        LEFT OUTER JOIN pg_locks ON (locktype = 'advisory'
                AND objsubid = 1
                AND ((classid::bigint << 32) | objid::bigint) = hash_text_to_bigint (format('benchbonanza-exp-%s@%s', a.epath, encode(a.commithash, 'hex')))
                AND DATABASE = (
                    SELECT
                        oid
                    FROM
                        pg_database
                WHERE
                    datname = current_database()))
);
COMMENT ON VIEW experiment_status IS 'Status of experiments: completed, not completed, running';

CREATE TABLE metric (
    id int UNIQUE GENERATED ALWAYS AS IDENTITY,
    mpath ltree UNIQUE NOT NULL
);
CREATE INDEX ON metric USING GIST (mpath gist_ltree_ops(siglen=64));
COMMENT ON TABLE metric IS 'Metric types. Deduplicated here; table `measurement` uses foreign keys to these.';
COMMENT ON COLUMN metric.mpath IS 'Labeltree metric type designation';


CREATE TABLE measurement (
    application_id integer NOT NULL REFERENCES application_run (application_id) ON DELETE CASCADE,
    at_t float4 NOT NULL,
    metric_id int NOT NULL REFERENCES metric (id) ON DELETE CASCADE,
    val bigint NOT NULL
);
CREATE INDEX ON measurement (application_id);
CREATE INDEX ON measurement (metric_id);
COMMENT ON TABLE measurement IS 'Measurements — time × metric × experiment';
COMMENT ON COLUMN measurement.at_t IS 'Time (s) into the experiment';


CREATE VIEW measurement_firstlast AS (
    WITH firstlast_annotated AS (
        SELECT
            *,
            row_number() OVER (PARTITION BY application_id,
                metric_id ORDER BY at_t ASC) AS rowno,
            row_number() OVER (PARTITION BY application_id,
                metric_id ORDER BY at_t DESC) AS rowno_rev
        FROM
            measurement
)
        SELECT
            application_id,
            at_t,
            metric_id,
            val,
            rowno = 1 AS is_first,
            rowno_rev = 1 AS is_last
        FROM
            firstlast_annotated
        WHERE (rowno = 1
            OR rowno_rev = 1)
);
COMMENT ON VIEW measurement_firstlast IS 'Measurements — only the first and last measurement for each metric × experiment'
