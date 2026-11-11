"""Deterministic generator for scalable synthetic CDC batches."""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def write_generated_events(output_path: str | Path, count: int, seed: int = 42) -> int:
    if count < 0:
        raise ValueError("count must be non-negative")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customers = max(1, count // 10)
    first_event = None
    written = 0
    with output.open("w", encoding="utf-8", newline="\n") as target:
        for index in range(count):
            customer = index % customers
            sequence = index // customers
            operation = "delete" if index and index % 29 == 0 else ("insert" if sequence == 0 else "update")
            event = {
                "event_id": f"evt-{index:09d}",
                "customer_id": f"customer-{customer:06d}",
                "operation": operation,
                "event_ts": (base + timedelta(seconds=sequence * 60 + rng.randint(-120, 120)))
                .isoformat().replace("+00:00", "Z"),
            }
            if operation != "delete":
                event.update({"customer_name": f"Customer {customer:06d}",
                              "email": f"customer{customer:06d}@example.test",
                              "status": "inactive" if index % 17 == 0 else "active"})
            if first_event is None:
                first_event = event
            target.write(json.dumps(event, sort_keys=True) + "\n")
            written += 1
        if first_event is not None:
            target.write(json.dumps(first_event, sort_keys=True) + "\n")
            written += 1
        invalid = [
            "{malformed json",
            json.dumps({"event_id": "invalid-missing", "customer_id": "customer-000000", "operation": "delete"}),
            json.dumps({"event_id": "invalid-operation", "customer_id": "customer-000000", "operation": "merge", "event_ts": "2025-01-01T00:00:00Z"}),
            json.dumps({"event_id": "invalid-key", "customer_id": "", "operation": "delete", "event_ts": "2025-01-01T00:00:00Z"}),
            json.dumps({"event_id": "invalid-schema", "customer_id": "customer-000000", "operation": "delete", "event_ts": "2025-01-01T00:00:00Z", "extra": True}),
        ]
        for record in invalid:
            target.write(record + "\n")
            written += 1
    return written