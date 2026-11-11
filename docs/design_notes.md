# Design Notes

SQLite and JSONL keep this vertical slice local and reproducible. A unique event ID provides idempotency; event history is append-only, while a separate current table changes only for the latest `(event_ts, event_id)`. Inserts and updates are complete snapshots so late events do not need an uncertain patch base. Invalid rows are quarantined; a fatal batch error rolls back accepted rows and rejects atomically while retaining a failed run record.

The timestamp ordering rule assumes source event times are trustworthy; the event ID is only a deterministic tie-breaker. SQLite is single-writer and is not intended for distributed ingestion. A production extension could use a warehouse merge, source schema versions, retention policies, and external alerting.