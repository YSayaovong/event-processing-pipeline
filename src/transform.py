import json


def transform(rows):
    """
    Input rows: list[dict] containing at least:
      event_id, event_ts, source, event_type, user_id, amount
    Ensures payload_json exists for raw load.
    """
    out = []
    for r in rows:
        payload = r.get("payload_json")

        # If CSV has no payload_json column, build one
        if payload is None:
            payload = json.dumps(
                {
                    "event_id": r.get("event_id"),
                    "event_ts": str(r.get("event_ts")),
                    "source": r.get("source"),
                    "event_type": r.get("event_type"),
                    "user_id": r.get("user_id"),
                    "amount": r.get("amount"),
                }
            )

        out.append(
            {
                "event_id": r.get("event_id"),
                "event_ts": r.get("event_ts"),
                "source": r.get("source"),
                "event_type": r.get("event_type"),
                "user_id": r.get("user_id"),
                "amount": r.get("amount"),
                "payload_json": payload,
            }
        )

    return out
