from datetime import datetime, timezone
from .load import update_watermark


def backfill(ts_iso: str):
    ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))

    # Ensure timezone-aware UTC
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    update_watermark(ts)


if __name__ == "__main__":
    backfill("2025-01-01T00:00:00+00:00")
