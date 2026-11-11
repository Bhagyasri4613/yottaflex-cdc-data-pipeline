# AI Prompt History

This file summarizes the user-provided prompts that guided the implementation; it is not a verbatim transcript.

1. **CDC challenge brief:** Build a local or clearly documented customer insert/update/delete pipeline with auditable history, deterministic handling of duplicates and out-of-order events, schema and business validation, rejects, metrics, tests, reproducible data, reconciliation, and performance notes.
2. **Run and explain:** Verify the project step by step, explain how to run it, and clarify how long verification would take.
3. **Output deliverables:** Produce a current-state dataset, event-history output, and a report with reconciliation results, and explain how to inspect those outputs.
4. **Windows setup issue:** Resolve PowerShell blocking virtual-environment activation and the resulting use of an unsupported global Python interpreter; provide commands that invoke the venv interpreter directly.
5. **Completion review:** Check the implementation against source/setup, sample/generator, tests/evidence, current and history outputs, reconciliation, performance, documentation, architecture/design, AI prompt history, and public GitHub/meaningful commit requirements.

The final verification used Python 3.12 from `.venv`, ran the automated tests, processed the 11-event sample, and exercised a generated 100,000-valid-event batch. See `README.md` and `data/sample_run_report.json` for reproducible commands and sample evidence.