# Data Dictionary

All column names use `snake_case`; timestamps are timezone-aware ISO-8601 normalized to UTC.

| Entity | Grain / key | Contract |
| --- | --- | --- |
| Source event | One customer snapshot event / `event_id` | Required `customer_id`, `operation` (`insert`, `update`, `delete`), and `event_ts`; inserts/updates require non-empty `customer_name`, `email`, `status`; deletes omit those fields. |
| `pipeline_runs` | One batch / `run_id` | Source path, UTC start/end, status, input/history/current/stale/duplicate/reject counts, fatal error text. |
| `event_history` | One accepted unique event / `event_id` | Immutable event payload, source path, and ingest run foreign key. |
| `customer_current` | One customer / `customer_id` | Latest snapshot, deletion flag, timestamp and event ID; latest event reference is a foreign key. |
| `rejected_events` | One invalid line / `reject_id` | Run, source path, 1-based line number, raw line, rejection reason, UTC time. |

`customer_id` is the business key. Unknown customers can first arrive in a late update/delete, so there is no separate customer-master FK. Current state sorts events by `(event_ts, event_id)`. A delete retains known payload as a tombstone; an unknown delete creates a null-payload tombstone.

The optional `--history-output` CSV exports all accepted unique events, ordered by customer and event time, including their source path and ingest `run_id`.