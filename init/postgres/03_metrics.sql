CREATE TABLE IF NOT EXISTS cdc_pipeline_metrics (
    id               SERIAL PRIMARY KEY,
    doc_id           TEXT,
    collection       TEXT,
    operation        TEXT,
    doc_size_bytes   INT,
    mongo_ts_ms      BIGINT,
    kafka_ts_ms      BIGINT,
    consumer_recv_ms BIGINT,
    pg_stored_ms     BIGINT,
    debezium_lat_ms  INT,
    consumer_lat_ms  INT,
    write_lat_ms     INT,
    e2e_lat_ms       INT,
    recorded_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cdc_metrics_recorded ON cdc_pipeline_metrics (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_cdc_metrics_col      ON cdc_pipeline_metrics (collection);
