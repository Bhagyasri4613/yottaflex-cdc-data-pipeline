"""Command-line interface for generating and processing CDC batches."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .generator import write_generated_events
from .pipeline import (
	build_reconciliation_report,
	export_current_state,
	export_event_history,
	export_rejects,
	process_file,
)


def main() -> None:
	parser = argparse.ArgumentParser(prog="cdc-pipeline")
	commands = parser.add_subparsers(dest="command", required=True)
	run = commands.add_parser("run", help="process a JSONL batch")
	run.add_argument("--input", type=Path, required=True)
	run.add_argument("--db", type=Path, default=Path("output/cdc.sqlite"))
	run.add_argument("--rejects", type=Path)
	run.add_argument("--current-output", type=Path)
	run.add_argument("--history-output", type=Path)
	run.add_argument("--report", type=Path)
	run.add_argument("--run-id")
	generate = commands.add_parser("generate", help="generate deterministic synthetic events")
	generate.add_argument("--output", type=Path, default=Path("data/generated_events.jsonl"))
	generate.add_argument("--count", type=int, default=100_000)
	generate.add_argument("--seed", type=int, default=42)
	args = parser.parse_args()

	if args.command == "generate":
		print(json.dumps({"output": str(args.output),
						  "records_written": write_generated_events(args.output, args.count, args.seed)}, indent=2))
		return

	started = time.perf_counter()
	result = process_file(args.input, args.db, args.run_id)
	duration = time.perf_counter() - started
	if args.rejects:
		export_rejects(args.db, result["run_id"], args.rejects)
	if args.current_output:
		export_current_state(args.db, args.current_output)
	if args.history_output:
		export_event_history(args.db, args.history_output)
	report = build_reconciliation_report(args.db, result, duration, args.current_output)
	if args.report:
		args.report.parent.mkdir(parents=True, exist_ok=True)
		args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	print(json.dumps(report["metrics"], indent=2, sort_keys=True))


if __name__ == "__main__":
	main()
