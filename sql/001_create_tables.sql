-- ETL run log
CREATE TABLE IF NOT EXISTS etl_runs (
  run_id           UUID PRIMARY KEY,
  pipeline_name    TEXT NOT NULL,
  started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  status           TEXT NOT NULL CHECK (status IN ('running','success','failed')),
  message          TEXT,
  extracted_count  INT DEFAULT 0,
  loaded_raw_count INT DEFAULT 0,
  upserted_count   INT DEFAULT 0
);

-- Watermark state per pipeline + source
CREATE TABLE IF NOT EXISTS etl_watermarks (
  pipeline_name TEXT NOT NULL,
  source_name   TEXT NOT NULL,
  watermark_ts  TIMESTAMPTZ NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (pipeline_name, source_name)
);

-- Raw ingestion table (idempotent via unique event_id)
CREATE TABLE IF NOT EXISTS raw_events (
  event_id     TEXT PRIMARY KEY,
  event_ts     TIMESTAMPTZ NOT NULL,
  source       TEXT NOT NULL,
  event_type   TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Curated fact table (upsert target)
CREATE TABLE IF NOT EXISTS fact_events (
  event_id     TEXT PRIMARY KEY,
  event_ts     TIMESTAMPTZ NOT NULL,
  source       TEXT NOT NULL,
  event_type   TEXT NOT NULL,
  user_id      TEXT,
  amount       NUMERIC,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON raw_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_fact_events_ts ON fact_events(event_ts);
