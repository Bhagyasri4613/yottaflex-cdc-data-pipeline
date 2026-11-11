from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cdc_pipeline.generator import write_generated_events
from cdc_pipeline.pipeline import (
	build_reconciliation_report,
	export_current_state,
	export_event_history,
	process_file,
)


def make_event(event_id: str, customer_id: str, operation: str, timestamp: str, name: str = "Example") -> dict:
	event = {"event_id": event_id, "customer_id": customer_id,
			 "operation": operation, "event_ts": timestamp}
	if operation != "delete":
		event.update({"customer_name": name, "email": "example@example.test", "status": "active"})
	return event


class PipelineTests(unittest.TestCase):
	def setUp(self) -> None:
		self.temp = tempfile.TemporaryDirectory()
		self.root = Path(self.temp.name)
		self.database = self.root / "test.sqlite"
		self.input = self.root / "events.jsonl"

	def tearDown(self) -> None:
		self.temp.cleanup()

	def write(self, records: list[dict | str]) -> None:
		with self.input.open("w", encoding="utf-8") as target:
			for record in records:
				target.write(record if isinstance(record, str) else json.dumps(record))
				target.write("\n")

	def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
		connection = sqlite3.connect(self.database)
		connection.row_factory = sqlite3.Row
		try:
			return connection.execute(sql, params).fetchall()
		finally:
			connection.close()

	def test_duplicates_and_replay_are_idempotent(self) -> None:
		record = make_event("e1", "c1", "insert", "2025-01-01T00:00:00Z")
		self.write([record, record])
		first = process_file(self.input, self.database, "run1")
		replay = process_file(self.input, self.database, "run2")
		self.assertEqual((first["history_inserted_count"], first["duplicate_count"]), (1, 1))
		self.assertEqual((replay["history_inserted_count"], replay["duplicate_count"]), (0, 2))
		self.assertEqual(self.query("SELECT COUNT(*) AS n FROM event_history")[0]["n"], 1)

	def test_empty_batch(self) -> None:
		self.input.write_text("", encoding="utf-8")
		result = process_file(self.input, self.database, "empty")
		self.assertEqual(result["status"], "success")
		self.assertEqual(result["input_count"], 0)

	def test_bad_json_missing_null_unknown_and_blank_key_are_rejected(self) -> None:
		self.write([
			"{bad json",
			{"event_id": "missing", "customer_id": "c1", "operation": "delete"},
			{**make_event("null", "c1", "update", "2025-01-01T00:00:00Z"), "email": None},
			make_event("blank", "", "delete", "2025-01-01T00:00:00Z"),
			{**make_event("extra", "c1", "delete", "2025-01-01T00:00:00Z"), "unknown": 1},
		])
		result = process_file(self.input, self.database, "bad")
		self.assertEqual(result["rejected_count"], 5)
		self.assertEqual(self.query("SELECT COUNT(*) AS n FROM rejected_events")[0]["n"], 5)

	def test_out_of_order_events_and_delete_tombstone(self) -> None:
		self.write([
			make_event("new", "c1", "update", "2025-01-03T00:00:00Z", "New"),
			make_event("old", "c1", "insert", "2025-01-01T00:00:00Z", "Old"),
			make_event("delete", "c1", "delete", "2025-01-04T00:00:00Z"),
			make_event("late", "c1", "update", "2025-01-02T00:00:00Z", "Late"),
		])
		result = process_file(self.input, self.database, "order")
		current = self.query("SELECT * FROM customer_current")[0]
		self.assertEqual((result["stale_count"], current["last_event_id"], current["is_deleted"]), (2, "delete", 1))
		self.assertEqual(current["customer_name"], "New")

	def test_conflicting_event_id_is_rejected(self) -> None:
		self.write([
			make_event("same", "c1", "insert", "2025-01-01T00:00:00Z", "A"),
			make_event("same", "c1", "insert", "2025-01-01T00:00:00Z", "B"),
		])
		result = process_file(self.input, self.database, "conflict")
		self.assertEqual(result["rejected_count"], 1)

	def test_non_string_operation_and_status_are_rejected(self) -> None:
		invalid_operation = make_event("bad-operation", "c1", "delete", "2025-01-01T00:00:00Z")
		invalid_operation["operation"] = []
		invalid_status = make_event("bad-status", "c2", "update", "2025-01-01T00:00:00Z")
		invalid_status["status"] = []
		self.write([invalid_operation, invalid_status])

		result = process_file(self.input, self.database, "bad-types")

		self.assertEqual(result["rejected_count"], 2)
		self.assertEqual(result["status"], "success_with_rejects")

	def test_equal_timestamp_tiebreaker_is_deterministic(self) -> None:
		timestamp = "2025-01-01T00:00:00Z"
		self.write([make_event("a", "c1", "insert", timestamp, "A"),
					make_event("z", "c1", "update", timestamp, "Z")])
		process_file(self.input, self.database, "tie")
		self.assertEqual(self.query("SELECT last_event_id FROM customer_current")[0][0], "z")

	def test_generator_is_repeatable_and_adds_scenarios(self) -> None:
		first, second = self.root / "a.jsonl", self.root / "b.jsonl"
		self.assertEqual(write_generated_events(first, 20, 7), 26)
		self.assertEqual(write_generated_events(second, 20, 7), 26)
		self.assertEqual(first.read_bytes(), second.read_bytes())

	def test_fatal_partial_read_rolls_back_and_retry_succeeds(self) -> None:
		valid = json.dumps(make_event("retry", "c1", "insert", "2025-01-01T00:00:00Z"))
		self.input.write_bytes((valid + "\n").encode("utf-8") + b"\xff\n")

		with self.assertRaises(UnicodeDecodeError):
			process_file(self.input, self.database, "failed-run")
		self.assertEqual(self.query("SELECT COUNT(*) AS n FROM event_history")[0]["n"], 0)
		failed = self.query("SELECT status, failure_reason FROM pipeline_runs WHERE run_id='failed-run'")[0]
		self.assertEqual(failed["status"], "failed")
		self.assertIn("utf-8", failed["failure_reason"])

		self.input.write_text(valid + "\n", encoding="utf-8")
		retried = process_file(self.input, self.database, "retry-run")
		self.assertEqual(retried["status"], "success")
		self.assertEqual(self.query("SELECT COUNT(*) AS n FROM event_history")[0]["n"], 1)

	def test_current_export_and_reconciliation_report(self) -> None:
		result = process_file(Path(__file__).parents[1] / "data" / "sample_events.jsonl", self.database, "export")
		current_path = self.root / "current.csv"
		history_path = self.root / "history.csv"
		export_current_state(self.database, current_path)
		export_event_history(self.database, history_path)
		report = build_reconciliation_report(self.database, result, 0.1, current_path)

		self.assertTrue(report["reconciliation"]["all_checks_passed"])
		self.assertEqual(report["metrics"]["output_count"], 2)
		self.assertEqual(len(current_path.read_text(encoding="utf-8").splitlines()), 3)
		self.assertEqual(len(history_path.read_text(encoding="utf-8").splitlines()), 7)

	def test_sample_reconciliation(self) -> None:
		sample = Path(__file__).parents[1] / "data" / "sample_events.jsonl"
		result = process_file(sample, self.database, "sample")
		self.assertEqual(
			[result[key] for key in ("input_count", "history_inserted_count", "current_applied_count",
									 "stale_count", "duplicate_count", "rejected_count")],
			[11, 6, 4, 2, 1, 4],
		)


if __name__ == "__main__":
	unittest.main()
