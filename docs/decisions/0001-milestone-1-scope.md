# ADR 0001: Milestone 1 scope

## Status

Accepted

## Context

QuantaFlow's full scope spans multiple sources, four data layers, Airflow orchestration,
a Postgres warehouse, a quality framework and benchmarking. Building all of it at once
makes early mistakes expensive to unwind and hides whether the foundational pieces
(manifesting, checksumming, idempotency) actually work.

## Decision

Implement only the first checkpoint: one TLC Yellow Taxi source connector, download,
checksum, manifest registration, MinIO raw storage and schema validation for a single
month (2024-01). Bronze, silver, gold, the second and third sources, the quality
framework, benchmarks and the full Airflow DAG are deliberately deferred.

## Consequences

- The repository structure follows the full target layout from the start, so later
  milestones extend existing modules rather than restructuring.
- Placeholder modules (`transformation`, `warehouse`, `monitoring`) exist as empty
  packages with a one-line docstring rather than stub implementations, to avoid dead
  code that would need to be deleted later.
- The Airflow DAG only wires the tasks that have real implementations; later tasks are
  documented as a comment rather than defined as unused stub functions.
