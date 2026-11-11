"""Transactional SQLite implementation of a JSONL customer CDC pipeline."""

from __future__ import annotations

import csv
import json
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BASE_FIELDS = {"event_id", "customer_id", "operation", "event_ts"}
PAYLOAD_FIELDS = {"customer_name", "email", "status"}
ALLOWED_FIELDS = BASE_FIELDS | PAYLOAD_FIELDS
OPERATIONS = {"insert", "update", "delete"}


def utc_now() -> str:
	return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def connect(database_path: str | Path) -> sqlite3.Connection:
	path = Path(database_path)
	path.parent.mkdir(parents=True, exist_ok=True)
	connection = sqlite3.connect(path)
	connection.row_factory = sqlite3.Row
	connection.execute("PRAGMA foreign_keys = ON")
	return connection


def initialize_database(database_path: str | Path) -> None:
	with closing(connect(database_path)) as connection:
		connection.executescript(
			"""
			CREATE TABLE IF NOT EXISTS pipeline_runs (
				run_id TEXT PRIMARY KEY, source_file TEXT NOT NULL,
				started_at TEXT NOT NULL, ended_at TEXT, status TEXT NOT NULL,
				input_count INTEGER NOT NULL DEFAULT 0,
				history_inserted_count INTEGER NOT NULL DEFAULT 0,
				current_applied_count INTEGER NOT NULL DEFAULT 0,
				stale_count INTEGER NOT NULL DEFAULT 0,
				duplicate_count INTEGER NOT NULL DEFAULT 0,
				rejected_count INTEGER NOT NULL DEFAULT 0, failure_reason TEXT
			);
			CREATE TABLE IF NOT EXISTS event_history (
				event_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL,
				operation TEXT NOT NULL CHECK (operation IN ('insert','update','delete')),
				event_ts TEXT NOT NULL, customer_name TEXT, email TEXT, status TEXT,
				event_json TEXT NOT NULL, source_file TEXT NOT NULL,
				run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id)
			);
			CREATE INDEX IF NOT EXISTS idx_history_customer_time
				ON event_history(customer_id, event_ts, event_id);
			CREATE TABLE IF NOT EXISTS customer_current (
				customer_id TEXT PRIMARY KEY, customer_name TEXT, email TEXT, status TEXT,
				is_deleted INTEGER NOT NULL CHECK (is_deleted IN (0,1)),
				last_event_ts TEXT NOT NULL,
				last_event_id TEXT NOT NULL REFERENCES event_history(event_id)
			);
			CREATE TABLE IF NOT EXISTS rejected_events (
				reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
				run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id),
				source_file TEXT NOT NULL, row_number INTEGER NOT NULL,
				raw_record TEXT NOT NULL, reason TEXT NOT NULL, rejected_at TEXT NOT NULL
			);
			CREATE INDEX IF NOT EXISTS idx_rejected_run ON rejected_events(run_id, row_number);
			"""
		)


def _normalize_timestamp(value: Any) -> str:
	if not isinstance(value, str) or not value.strip():
		raise ValueError("event_ts must be a non-empty ISO-8601 timestamp")
	try:
		parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
	except ValueError as error:
		raise ValueError("event_ts must be a valid ISO-8601 timestamp") from error
	if parsed.tzinfo is None:
		raise ValueError("event_ts must include a timezone")
	return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate_event(value: Any) -> dict[str, Any]:
	if not isinstance(value, dict):
		raise ValueError("record must be a JSON object")
	unexpected = set(value) - ALLOWED_FIELDS
	if unexpected:
		raise ValueError(f"unexpected field(s): {', '.join(sorted(unexpected))}")
	missing = BASE_FIELDS - set(value)
	if missing:
		raise ValueError(f"missing required field(s): {', '.join(sorted(missing))}")
	event = dict(value)
	for field in ("event_id", "customer_id"):
		if not isinstance(event[field], str) or not event[field].strip():
			raise ValueError(f"{field} must be a non-empty string")
		event[field] = event[field].strip()
	if not isinstance(event["operation"], str) or event["operation"] not in OPERATIONS:
		raise ValueError("operation must be insert, update, or delete")
	event["event_ts"] = _normalize_timestamp(event["event_ts"])

	if event["operation"] in {"insert", "update"}:
		absent = PAYLOAD_FIELDS - set(event)
		if absent:
			raise ValueError(f"missing required field(s): {', '.join(sorted(absent))}")
		for field in PAYLOAD_FIELDS:
			if not isinstance(event[field], str) or not event[field].strip():
				raise ValueError(f"{field} must be a non-empty string")
			event[field] = event[field].strip()
		if not isinstance(event["status"], str) or event["status"] not in {"active", "inactive"}:
			raise ValueError("status must be active or inactive")
	else:
		for field in PAYLOAD_FIELDS:
			if event.get(field) is not None:
				raise ValueError(f"delete events cannot include {field}")
			event[field] = None
	return event


def _reject(
	connection: sqlite3.Connection, run_id: str, source_file: str,
	row_number: int, raw_record: str, reason: str,
) -> None:
	connection.execute(
		"""INSERT INTO rejected_events
		   (run_id, source_file, row_number, raw_record, reason, rejected_at)
		   VALUES (?, ?, ?, ?, ?, ?)""",
		(run_id, source_file, row_number, raw_record, reason, utc_now()),
	)


def process_file(
	input_path: str | Path,
	database_path: str | Path,
	run_id: str | None = None,
) -> dict[str, Any]:
	"""Load a JSONL batch atomically, retaining history and quarantining bad rows."""
	source_path = Path(input_path)
	source_file = str(source_path)
	run_id = run_id or str(uuid.uuid4())
	counters = {
		"input_count": 0, "history_inserted_count": 0,
		"current_applied_count": 0, "stale_count": 0,
		"duplicate_count": 0, "rejected_count": 0,
	}
	initialize_database(database_path)
	connection = connect(database_path)
	connection.execute(
		"INSERT INTO pipeline_runs (run_id, source_file, started_at, status) VALUES (?, ?, ?, 'running')",
		(run_id, source_file, utc_now()),
	)
	connection.commit()
	try:
		connection.execute("BEGIN IMMEDIATE")
		with source_path.open("r", encoding="utf-8") as source:
			for row_number, raw_record in enumerate(source, 1):
				counters["input_count"] += 1
				try:
					event = validate_event(json.loads(raw_record))
				except (json.JSONDecodeError, ValueError) as error:
					_reject(connection, run_id, source_file, row_number, raw_record.rstrip("\r\n"), str(error))
					counters["rejected_count"] += 1
					continue

				event_json = json.dumps(event, sort_keys=True, separators=(",", ":"))
				prior = connection.execute(
					"SELECT event_json FROM event_history WHERE event_id = ?", (event["event_id"],)
				).fetchone()
				if prior:
					if prior["event_json"] == event_json:
						counters["duplicate_count"] += 1
					else:
						_reject(
							connection, run_id, source_file, row_number,
							raw_record.rstrip("\r\n"), "event_id reused with different content",
						)
						counters["rejected_count"] += 1
					continue

				connection.execute(
					"""INSERT INTO event_history
					   (event_id, customer_id, operation, event_ts, customer_name, email,
						status, event_json, source_file, run_id)
					   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
					(event["event_id"], event["customer_id"], event["operation"], event["event_ts"],
					 event["customer_name"], event["email"], event["status"], event_json,
					 source_file, run_id),
				)
				counters["history_inserted_count"] += 1
				current = connection.execute(
					"""SELECT customer_name, email, status, last_event_ts, last_event_id
					   FROM customer_current WHERE customer_id = ?""",
					(event["customer_id"],),
				).fetchone()
				incoming_order = (event["event_ts"], event["event_id"])
				current_order = (current["last_event_ts"], current["last_event_id"]) if current else None
				if current_order is not None and incoming_order <= current_order:
					counters["stale_count"] += 1
					continue

				if event["operation"] == "delete":
					name = current["customer_name"] if current else None
					email = current["email"] if current else None
					status = current["status"] if current else None
					is_deleted = 1
				else:
					name, email, status = event["customer_name"], event["email"], event["status"]
					is_deleted = 0
				connection.execute(
					"""INSERT INTO customer_current
					   (customer_id, customer_name, email, status, is_deleted, last_event_ts, last_event_id)
					   VALUES (?, ?, ?, ?, ?, ?, ?)
					   ON CONFLICT(customer_id) DO UPDATE SET
						   customer_name=excluded.customer_name, email=excluded.email,
						   status=excluded.status, is_deleted=excluded.is_deleted,
						   last_event_ts=excluded.last_event_ts, last_event_id=excluded.last_event_id""",
					(event["customer_id"], name, email, status, is_deleted,
					 event["event_ts"], event["event_id"]),
				)
				counters["current_applied_count"] += 1

		status = "success_with_rejects" if counters["rejected_count"] else "success"
		connection.execute(
			"""UPDATE pipeline_runs SET ended_at=?, status=?, input_count=?,
			   history_inserted_count=?, current_applied_count=?, stale_count=?,
			   duplicate_count=?, rejected_count=? WHERE run_id=?""",
			(utc_now(), status, counters["input_count"], counters["history_inserted_count"],
			 counters["current_applied_count"], counters["stale_count"],
			 counters["duplicate_count"], counters["rejected_count"], run_id),
		)
		connection.commit()
	except Exception as error:
		connection.rollback()
		connection.execute(
			"UPDATE pipeline_runs SET ended_at=?, status='failed', input_count=?, failure_reason=? WHERE run_id=?",
			(utc_now(), counters["input_count"], str(error)[:1000], run_id),
		)
		connection.commit()
		raise
	finally:
		connection.close()
	return {"run_id": run_id, "status": status, **counters}


def export_rejects(database_path: str | Path, run_id: str, output_path: str | Path) -> None:
	output = Path(output_path)
	output.parent.mkdir(parents=True, exist_ok=True)
	with closing(connect(database_path)) as connection, output.open("w", encoding="utf-8", newline="\n") as target:
		rows = connection.execute(
			"SELECT source_file, row_number, raw_record, reason FROM rejected_events WHERE run_id=? ORDER BY row_number",
			(run_id,),
		)
		for row in rows:
			target.write(json.dumps(dict(row), sort_keys=True) + "\n")


def export_current_state(database_path: str | Path, output_path: str | Path) -> None:
	fields = ["customer_id", "customer_name", "email", "status", "is_deleted", "last_event_ts", "last_event_id"]
	output = Path(output_path)
	output.parent.mkdir(parents=True, exist_ok=True)
	with closing(connect(database_path)) as connection, output.open("w", encoding="utf-8", newline="") as target:
		writer = csv.DictWriter(target, fieldnames=fields)
		writer.writeheader()
		rows = connection.execute(f"SELECT {', '.join(fields)} FROM customer_current ORDER BY customer_id")
		writer.writerows(dict(row) for row in rows)


def export_event_history(database_path: str | Path, output_path: str | Path) -> None:
	fields = [
		"event_id", "customer_id", "operation", "event_ts", "customer_name",
		"email", "status", "source_file", "run_id",
	]
	output = Path(output_path)
	output.parent.mkdir(parents=True, exist_ok=True)
	with closing(connect(database_path)) as connection, output.open("w", encoding="utf-8", newline="") as target:
		writer = csv.DictWriter(target, fieldnames=fields)
		writer.writeheader()
		rows = connection.execute(
			f"SELECT {', '.join(fields)} FROM event_history ORDER BY customer_id, event_ts, event_id"
		)
		writer.writerows(dict(row) for row in rows)


def build_reconciliation_report(
	database_path: str | Path,
	run_result: dict[str, Any],
	duration_seconds: float,
	current_output: str | Path | None = None,
) -> dict[str, Any]:
	with closing(connect(database_path)) as connection:
		run = connection.execute(
			"SELECT source_file, started_at, ended_at, status, failure_reason FROM pipeline_runs WHERE run_id=?",
			(run_result["run_id"],),
		).fetchone()
		operations = connection.execute(
			"SELECT operation, COUNT(*) AS n FROM event_history WHERE run_id=? GROUP BY operation ORDER BY operation",
			(run_result["run_id"],),
		).fetchall()
		snapshot = connection.execute(
			"SELECT COUNT(*) AS rows, COALESCE(SUM(is_deleted),0) AS deleted FROM customer_current"
		).fetchone()
	input_ok = run_result["input_count"] == (
		run_result["history_inserted_count"] + run_result["duplicate_count"] + run_result["rejected_count"]
	)
	history_ok = run_result["history_inserted_count"] == (
		run_result["current_applied_count"] + run_result["stale_count"]
	)
	report: dict[str, Any] = {
		"run": {"run_id": run_result["run_id"], "source_file": run["source_file"],
				"started_at": run["started_at"], "ended_at": run["ended_at"],
				"status": run["status"], "failure_reason": run["failure_reason"]},
		"metrics": {**run_result, "output_count": snapshot["rows"],
					"duration_seconds": round(duration_seconds, 6),
					"input_records_per_second": round(run_result["input_count"] / duration_seconds, 2)
					if duration_seconds else None},
		"event_history_by_operation": {row["operation"]: row["n"] for row in operations},
		"database_snapshot": {"current_row_count": snapshot["rows"], "deleted_row_count": snapshot["deleted"]},
		"reconciliation": {
			"input_equals_history_plus_duplicates_plus_rejects": input_ok,
			"history_equals_current_applied_plus_stale": history_ok,
			"all_checks_passed": input_ok and history_ok,
		},
	}
	if current_output is not None:
		report["current_state_csv"] = str(current_output)
	return report
