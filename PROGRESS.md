# Progress log

Add a new entry at the end of every session (and whenever you stop working).

# Progress log

Add a new entry at the end of every session (and whenever you stop working).

## Mon 05 Oct (Lesson 9)
- Done: nothing committed yet (project work started Sun 18 Oct)
- Next: setup and Checkpoint A
- Blocked: nothing

## Thu 08 Oct (Lesson 10)
- Done: nothing committed yet
- Next: setup and Checkpoint A
- Blocked: nothing

## Sun 18 Oct
- Done: set up on Windows 11 / PowerShell (venv, .env with my UVA_ID, four terminals). pip install hung on dbt-core-experimental-parser building from source; it finished after waiting. smoke_test.py prints Emulator OK. Checkpoint A: populate_queue (POST, logs queue URL), get_counts and monitor_queue (logs visible / in-flight / delayed until delayed = 0). Flow run Completed in the Prefect dashboard. Fixed .env still using the placeholder ID mst3k.
- Next: DAG sketch, Checkpoint B
- Blocked: nothing

## Mon 19 Oct (Lesson 11)
- Done: nothing; no wifi at the library or school. Instructor gave a one-day extension to Tue 20 Oct 23:59.
- Next: finish B, C, D, chaos round, full-delay run, README
- Blocked: no internet access

## Tue 20 Oct
- Done:
  - Fixed `prefect server start` failing with "An Application Control policy has blocked this file" (Windows Smart App Control blocking SQLAlchemy 2.1's compiled extension): pinned sqlalchemy>=2.0.40,<2.1.
  - Committed Checkpoint A with the DAG sketch (docs/dag.png) and dashboard screenshot.
  - Checkpoint B: prepare_landing creates raw.fragments; collect_messages receives with MessageAttributeNames=["All"] and resp.get("Messages", []), inserts into DuckDB before delete_message, stops at 21 distinct fragments with all counters at 0, 1,200 s timeout. Forced a 15 s timeout to show the clear TimeoutError.
  - Checkpoint C: stg_fragments (integer cast, trim, distinct), generic tests (not_null, unique), singular test assert_complete_sequence, assembled_phrase mart; dbt_build task raises on failure. Made tests fail on purpose (deleted order_no 7; added a second word for order_no 7), then passed again. Fixed two empty dbt files that hadn't been saved, and mislabelled screenshots.
  - Checkpoint D: submit_solution sends uvaid / phrase / platform to dp2-submit and checks HTTP 200. check_submission.py: PASS, receipt F3C00E08C5.
  - Chaos round: the poison message crashed collect_messages with KeyError: 'word'; fixed by logging and deleting malformed messages and logging duplicates (counted once). Re-runs then hit HTTP 500 (PurgeQueueInProgress: one purge per 60 s); changed populate_queue retries to a 65 s delay. Two consecutive chaos runs ended in PASS.
  - Full-delay evidence run (DP2_FAST=0, 30-900 s): completed end to end in about 15 minutes, dbt PASS=6, submitted HTTP 200.
  - Wrote the README (setup, DAG, polling strategy, receive -> save -> delete, dbt gate, chaos round, tool roles, results) and the self-assessed RUBRIC.md.
- Next: submit repo link and rubric on Canvas
- Blocked: nothing