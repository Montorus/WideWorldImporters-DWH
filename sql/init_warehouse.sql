-- Pipeline runs metadata table
-- Tracks every DAG execution for data freshness monitoring

CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    id              SERIAL PRIMARY KEY,
    dag_name        TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    rows_loaded     INTEGER,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          TEXT,
    error_message   TEXT
);