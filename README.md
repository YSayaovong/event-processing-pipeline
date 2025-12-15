# Event Processing Pipeline

## Overview

This repository contains a **production-style incremental event processing pipeline** built with Python and PostgreSQL.

The pipeline ingests application events from a CSV source, loads them idempotently into a raw layer, transforms them into a curated fact table, and tracks operational metadata such as run status, row counts, retries, and execution duration.

The design emphasizes **correctness, rerunnability, and operational visibility**, mirroring how real data pipelines are built and operated in production environments.

---

## Key Features

### Incremental Processing
- Watermark-based extraction ensures only new or late-arriving events are processed
- Configurable late-arrival grace window
- Safe backfill support by resetting watermarks

### Idempotent Loads
- Raw events are deduplicated using primary keys
- Curated fact table uses upserts (`ON CONFLICT DO UPDATE`)
- Pipeline can be re-run without data corruption

### Operational Tracking
- Each pipeline execution is recorded in `pipeline_runs`
- Captures:
  - run status (`running`, `success`, `failed`)
  - row counts (extracted, raw loaded, upserted)
  - retry attempts
  - execution duration in milliseconds
  - error messages when failures occur

### Resilience & Reliability
- Retry logic for transient database failures with exponential backoff
- Fail-fast behavior for data or logic errors
- Clean failure recording even during partial outages

### Environment-Driven Configuration
- All configuration managed via `.env`
- No hardcoded credentials or environment-specific values
- Easy portability between environments

---

## Architecture

```
data/raw/events.csv
        |
        v
+----------------+
|   Extract      |  <- watermark + late-arrival handling
+----------------+
        |
        v
+----------------+
|   Raw Load     |  -> raw_events (idempotent)
+----------------+
        |
        v
+----------------+
|  Transform     |
+----------------+
        |
        v
+----------------+
| Curated Load   |  -> fact_events (upserts)
+----------------+
        |
        v
+----------------+
| Run Tracking   |  -> pipeline_runs, pipeline_watermarks
+----------------+
```

---

## Database Tables

- `raw_events` — immutable raw ingestion layer
- `fact_events` — curated, query-ready fact table
- `pipeline_runs` — operational metadata for every run
- `pipeline_watermarks` — per-pipeline extraction state

---

## Running the Pipeline

### Prerequisites
- Python 3.12+
- PostgreSQL
- Virtual environment (`.venv`)

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run the pipeline
```bash
python -m src.etl
```

The pipeline is safe to run multiple times.

---

## Backfill Support

To reprocess historical data:

```bash
python -m src.backfill
python -m src.etl
```

---

## Configuration

All configuration is managed via `.env`:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/event_processing
PIPELINE_NAME=event-processing-pipeline
SOURCE_NAME=application_events
RAW_DATA_PATH=data/raw/events.csv
LATE_ARRIVAL_GRACE_MINUTES=60
FAIL_MODE=none
MAX_RETRIES=4
```

---

## Current Status

**This pipeline is complete and production-correct** for:
- incremental ingestion
- idempotent loading
- operational tracking
- safe re-execution

It reflects how real-world data pipelines are designed and operated at a foundational level.

---

## Future Improvements

### Phase 3 — Observability
- Structured JSON logging
- Step-level execution timing
- Data quality validation checks
- Dead-letter / quarantine table for invalid records

### Phase 4 — Deployment & DevOps
- Dockerized pipeline
- One-command startup
- Health checks
- CI pipeline (linting, tests, pipeline execution)
- Scheduled execution (cron / task scheduler)

### Phase 5 — Platform Integration
- Orchestration with Airflow or Dagster
- Metrics export for monitoring tools
- Infrastructure-as-code
- Secrets management

---
