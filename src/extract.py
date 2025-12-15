import csv
import pandas as pd
from datetime import timedelta

from .db import fetch_one
from .settings import settings


def effective_watermark():
    row = fetch_one(
        """
        SELECT watermark_ts
        FROM pipeline_watermarks
        WHERE pipeline_name=%s AND source_name=%s
        """,
        (settings.PIPELINE_NAME, settings.SOURCE_NAME),
    )

    base = row["watermark_ts"] if row else pd.Timestamp("2000-01-01", tz="UTC")
    return base - timedelta(minutes=settings.LATE_ARRIVAL_GRACE_MINUTES)


def _read_events_csv(path: str) -> pd.DataFrame:
    # Use python engine + csv module settings to handle quoted commas safely
    df = pd.read_csv(
        path,
        engine="python",
        dtype=str,
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        on_bad_lines="skip",   # skip malformed lines (we’ll surface count)
    )

    # Normalize known columns if present
    # Common layouts:
    # 5 cols: event_id,event_ts,source,event_type,user_id,amount  (actually 6)
    # 5 cols: event_id,event_ts,source,event_type,amount          (5)
    # 6 cols+: extra payload_json or other
    return df


def extract():
    df = _read_events_csv(settings.RAW_DATA_PATH)

    # If the CSV has no header row, pandas will name columns 0..N-1.
    # We handle both cases.
    cols = list(df.columns)

    # If columns are integers, assume no header and set a schema.
    if all(isinstance(c, int) for c in cols):
        # Try 6-column schema first (most likely from your earlier “e3,...,u2,50” line)
        if len(cols) >= 6:
            df = df.rename(
                columns={0: "event_id", 1: "event_ts", 2: "source", 3: "event_type", 4: "user_id", 5: "amount"}
            )
        else:
            df = df.rename(
                columns={0: "event_id", 1: "event_ts", 2: "source", 3: "event_type", 4: "amount"}
            )

    # If header exists but column names differ, standardize minimally
    # Ensure required columns exist
    required = {"event_id", "event_ts", "source", "event_type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {sorted(missing)}. Found columns: {list(df.columns)}")

    # Parse timestamps
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    df = df[df["event_ts"].notna()].copy()

    # Watermark filter
    wm = effective_watermark()
    df = df[df["event_ts"] >= wm].copy()

    # Build payload_json if not present (so raw load always works)
    if "payload_json" not in df.columns:
        df["payload_json"] = None

    # Ensure optional columns exist
    if "user_id" not in df.columns:
        df["user_id"] = None
    if "amount" not in df.columns:
        df["amount"] = None

    return df.to_dict(orient="records")
