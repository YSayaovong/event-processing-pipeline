import uuid
from datetime import datetime
from .db import get_conn, execute
from .settings import settings


def start_run() -> str:
    run_id = str(uuid.uuid4())
    execute(
        """
        INSERT INTO pipeline_runs (
            run_id,
            pipeline_name,
            status,
            started_at
        )
        VALUES (%s, %s, 'running', now())
        """,
        (run_id, settings.PIPELINE_NAME),
    )
    return run_id


def finish_run(
    run_id: str,
    status: str,
    message: str,
    extracted: int,
    raw: int,
    upserted: int,
    retry_attempts: int,
    duration_ms: int,
) -> None:
    execute(
        """
        UPDATE pipeline_runs
        SET
            finished_at = now(),
            status = %s,
            message = %s,
            extracted_count = %s,
            loaded_raw_count = %s,
            upserted_count = %s,
            retry_attempts = %s,
            duration_ms = %s
        WHERE run_id = %s
        """,
        (
            status,
            message,
            extracted,
            raw,
            upserted,
            retry_attempts,
            duration_ms,
            run_id,
        ),
    )


def load_raw(rows) -> int:
    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO raw_events (
                        event_id,
                        event_ts,
                        source,
                        event_type,
                        payload_json
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        r["event_id"],
                        r["event_ts"],
                        r["source"],
                        r["event_type"],
                        r.get("payload_json"),
                    ),
                )
                inserted += cur.rowcount
    return inserted


def upsert_fact(rows) -> int:
    upserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO fact_events (
                        event_id,
                        event_ts,
                        source,
                        event_type,
                        user_id,
                        amount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        amount = EXCLUDED.amount,
                        updated_at = now()
                    """,
                    (
                        r["event_id"],
                        r["event_ts"],
                        r["source"],
                        r["event_type"],
                        r.get("user_id"),
                        r.get("amount"),
                    ),
                )
                upserted += cur.rowcount
    return upserted


def update_watermark(ts: datetime) -> None:
    execute(
        """
        INSERT INTO pipeline_watermarks (
            pipeline_name,
            source_name,
            watermark_ts
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (pipeline_name, source_name)
        DO UPDATE
        SET
            watermark_ts = EXCLUDED.watermark_ts,
            updated_at = now()
        """,
        (settings.PIPELINE_NAME, settings.SOURCE_NAME, ts),
    )
