# Pipeline Operations

## Normal run (single month)

```bash
python scripts/run_milestone1.py --period 2024-01
```

This discovers the source URL, downloads the file, computes a SHA-256 checksum, validates
the schema and row count, uploads the raw file to MinIO and writes a manifest record.

## Incremental run

Re-running the same period after it has already been validated is a no-op: the manifest
store detects the matching dataset, period and checksum and the script exits early with
`skip_already_processed`. Running a new period only processes that period.

## Backfill

Milestone 1 backfill is a shell loop; Airflow-native backfill (looping the DAG over a date
range) is wired in once the DAG has more than one task:

```bash
for period in 2024-01 2024-02 2024-03 2024-04 2024-05 2024-06; do
  python scripts/run_milestone1.py --period "$period"
done
```

## Retry policy

- Network/download failures: safe to retry, no side effects before a manifest is written.
- Validation failures: not retried automatically; the manifest records `validation_failed`
  so the failure is visible and requires a schema or data fix before re-running.
- Configuration/programming errors: the script exits non-zero immediately, no manifest is
  written.

## Failure recovery

Because the manifest is only written as `validated` after checksum, validation and upload
succeed, a failed run never marks a period as processed. Re-running the same command is the
recovery path; no manual cleanup of MinIO or the manifest store is required for a failed
run, since a genuinely failed attempt does not create a `validated` record to conflict with.

## Local environment

`docker-compose.yml` starts Postgres, MinIO and Airflow (standalone, LocalExecutor).

```bash
cp .env.example .env
docker compose up -d
```

Airflow UI: http://localhost:8080
MinIO console: http://localhost:9001
