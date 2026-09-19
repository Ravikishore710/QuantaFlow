CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    data_interval_start DATE NOT NULL,
    data_interval_end DATE NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    rows_in BIGINT,
    rows_out BIGINT,
    rows_rejected BIGINT,
    code_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitoring.file_manifest (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    period TEXT NOT NULL,
    source_url TEXT NOT NULL,
    object_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    schema_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_processed_at TIMESTAMPTZ,
    processing_status TEXT NOT NULL,
    UNIQUE (source, period, sha256)
);

CREATE TABLE IF NOT EXISTS monitoring.quality_results (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES monitoring.pipeline_runs (run_id),
    dataset TEXT NOT NULL,
    partition TEXT NOT NULL,
    check_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    rows_checked BIGINT NOT NULL,
    violations BIGINT NOT NULL,
    status TEXT NOT NULL,
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_file_manifest_source_period
    ON monitoring.file_manifest (source, period);

CREATE INDEX IF NOT EXISTS idx_quality_results_run_id
    ON monitoring.quality_results (run_id);
