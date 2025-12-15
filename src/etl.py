import time
from psycopg import OperationalError

from .extract import extract
from .transform import transform
from .load import (
    start_run,
    finish_run,
    load_raw,
    upsert_fact,
    update_watermark,
)
from .settings import settings


BASE_BACKOFF_SECONDS = 1.0


def _sleep_backoff(attempt: int) -> None:
    time.sleep(BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))


def _is_transient_db_error(exc: Exception) -> bool:
    return isinstance(exc, OperationalError)


def _maybe_fail(mode: str) -> None:
    if mode == "db_transient":
        raise OperationalError("Simulated transient DB failure")
    if mode == "data_bad":
        raise ValueError("Simulated bad data failure")
    if mode == "crash":
        raise RuntimeError("Simulated unexpected crash")


def run():
    start_time = time.perf_counter()
    retry_attempts = 0
    extracted = raw = upserted = 0

    run_id = None

    # ---- Start pipeline run (with retries) ----
    for attempt in range(1, settings.MAX_RETRIES + 1):
        try:
            if settings.FAIL_MODE == "db_transient" and attempt == 1:
                _maybe_fail("db_transient")

            run_id = start_run()
            break

        except Exception as e:
            if _is_transient_db_error(e) and attempt < settings.MAX_RETRIES:
                retry_attempts += 1
                _sleep_backoff(attempt)
                continue
            raise

    if run_id is None:
        raise RuntimeError("Failed to start pipeline run")

    # ---- Execute pipeline ----
    try:
        rows = extract()
        extracted = len(rows)

        if extracted == 0:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            finish_run(
                run_id,
                "success",
                "No new data",
                0,
                0,
                0,
                retry_attempts,
                duration_ms,
            )
            return

        for attempt in range(1, settings.MAX_RETRIES + 1):
            try:
                if settings.FAIL_MODE == "crash" and attempt == 1:
                    _maybe_fail("crash")

                raw = load_raw(rows)

                if settings.FAIL_MODE == "data_bad":
                    _maybe_fail("data_bad")

                curated = transform(rows)
                upserted = upsert_fact(curated)

                max_ts = max(r["event_ts"] for r in rows)
                update_watermark(max_ts)

                duration_ms = int((time.perf_counter() - start_time) * 1000)
                finish_run(
                    run_id,
                    "success",
                    "OK",
                    extracted,
                    raw,
                    upserted,
                    retry_attempts,
                    duration_ms,
                )
                return

            except Exception as e:
                if not _is_transient_db_error(e):
                    raise

                if attempt < settings.MAX_RETRIES:
                    retry_attempts += 1
                    _sleep_backoff(attempt)
                    continue
                raise

    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)

        if _is_transient_db_error(e):
            msg = f"transient_db_error: {e}"
        else:
            msg = f"pipeline_error: {e}"

        try:
            finish_run(
                run_id,
                "failed",
                msg,
                extracted,
                raw,
                upserted,
                retry_attempts,
                duration_ms,
            )
        except Exception:
            pass

        raise


if __name__ == "__main__":
    run()
