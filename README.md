# Customer CDC Pipeline

A local change-data-capture pipeline that keeps customer event history, publishes current state, quarantines invalid rows, and records run metrics in SQLite. It uses only Python's standard library.

## Architecture

```text
JSONL producer -> parse and validate -> valid events -> append-only event_history
								   |                         |
								   +-> rejected_events       +-> event-time merge -> customer_current
															 +-> pipeline_runs metrics
```

Each input line is validated independently. Accepted unique events and rejected rows are written in one SQLite transaction. Valid but out-of-order events remain in history and do not replace newer current state.

## Run on Windows PowerShell

Requires Python 3.10+. **Activation is optional.** If PowerShell blocks `Activate.ps1`, do not activate the environment; call its Python executable directly as shown below. This also avoids accidentally using another Python version on `PATH`.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m cdc_pipeline.cli run --input data/sample_events.jsonl --db output/sample.sqlite --rejects output/rejects.jsonl --current-output data/sample_customer_current.csv --history-output data/sample_event_history.csv --report data/sample_run_report.json
```

Tests should report 11 tests and `OK`. The sample command prints run metrics; its report contains reconciliation booleans. `success_with_rejects` is expected because the sample intentionally includes malformed and invalid rows. Use a fresh database path to reproduce the first-run counts below; rerunning against an existing database is safe and will count existing events as duplicates. The CSV and JSON report are reproducible sample deliverables.

## CDC Behavior

Each JSONL line is a customer snapshot with `event_id`, `customer_id`, `operation`, and timezone-aware ISO-8601 `event_ts`. Inserts and updates require `customer_name`, `email`, and `status` (`active` or `inactive`). Deletes create tombstones. Unknown columns, null required values, malformed records, and invalid keys are quarantined with source path, row number, raw line, and reason.

Exact event IDs are idempotent. Reusing an event ID with different content is rejected. Accepted unique events are retained in `event_history`; current state advances only for a later `(event_ts, event_id)` pair. Older events remain in history but do not overwrite newer current state. A batch commits atomically; a fatal failure rolls back its data and leaves a failed run metric.

## Assumptions

- `event_id` is stable and globally unique for a logical source event.
- Insert and update payloads are complete after-state snapshots, not partial patches.
- `event_ts` is a trustworthy event-time timestamp with a timezone; equal timestamps are ordered by `event_id` for deterministic results.
- The pipeline handles one customer entity and allows a customer to first appear in an update or delete; there is no separate customer master-data source.

## Larger Reproducible Batch

```powershell
.\.venv\Scripts\python.exe -m cdc_pipeline.cli generate --count 100000 --seed 42 --output output/events.jsonl
.\.venv\Scripts\python.exe -m cdc_pipeline.cli run --input output/events.jsonl --db output/large.sqlite --rejects output/large_rejects.jsonl --current-output output/large_current.csv --report output/large_report.json
```

The generator adds one exact duplicate and five invalid examples. Its seed makes output deterministic. Runtime databases and large generated files are ignored by Git; the small sample input and expected outputs remain checked in.

## Outputs and Reconciliation

SQLite tables: `pipeline_runs`, `event_history`, `customer_current`, and `rejected_events`. For every committed batch:

- `input_count = history_inserted_count + duplicate_count + rejected_count`
- `history_inserted_count = current_applied_count + stale_count`

The sample is expected to contain 11 inputs, 6 history inserts, 4 current-state applications, 2 stale events, 1 duplicate, and 4 rejects. The report also includes event counts by operation and current-table row/tombstone totals. See [docs/data_dictionary.md](docs/data_dictionary.md) and [docs/design_notes.md](docs/design_notes.md).

Sample artifacts: [current-state CSV](data/sample_customer_current.csv), [event-history CSV](data/sample_event_history.csv), [run and reconciliation report](data/sample_run_report.json), and [quarantined records](data/sample_rejects.jsonl). The AI prompt summary is in [docs/ai_prompt_history.md](docs/ai_prompt_history.md).

## Known Limitations

SQLite serializes writers and this local implementation is intended for a single-writer demonstration, not distributed ingestion. The source must provide full snapshots and reliable event timestamps. Schema changes are rejected until the validator is updated; there is no remote storage, alerting, retention policy, or partial-patch merge.

## Next Steps

For a production deployment, replace SQLite with a managed warehouse/database merge, add versioned source schemas and operational alerting, define history retention, and benchmark concurrency and batch size against the target platform.

## Retry and Backfill

An exact file rerun against the same database is safe: already accepted event IDs are counted as duplicates and do not create new history or current-state changes. A fatal file/database error rolls back that batch's event, current-state, and reject writes; the failed run row and failure reason remain. Correct the source/environment and rerun with a new run ID (the default is generated automatically). Earlier committed runs are unaffected. To backfill, process older events into the same database: history is retained, and only events later than the current `(event_ts, event_id)` advance current state. Use a new database path for a full rebuild.

## Performance Note

The CLI records elapsed seconds and input records per second. A local 100,000-valid-event run completed in about 3.7 seconds (roughly 26,700 input records/second); treat this as an environment-specific observation, not an SLA. One reasonable next optimization for larger batches is staging and bulk upsert work to reduce per-event SQLite reads while preserving the same transaction and ordering rules.
