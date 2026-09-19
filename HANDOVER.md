# QuantaFlow — Agent Handover

## What this covers

`quanta-flow.zip` contains the full repository scaffold plus a working implementation of
**Milestone 1** (spec section 82): one real source, downloaded, hashed, manifested, stored
and validated, wired end to end. Everything else in the 82-point spec is either scaffolded
(directory exists, contract/DDL defined) or explicitly deferred. This document says exactly
which is which, so the next agent or session doesn't have to re-derive it from the spec.

Nothing here was run against live infrastructure. This sandbox has no network access and no
Docker, so I could not actually download from `d37ci6vzurychx.cloudfront.net`, spin up
Postgres/MinIO/Airflow, or execute the pipeline end to end. All code compiles
(`python -m py_compile` clean across every file) and the test suite is written to run against
local fixtures with no network dependency, but **you need to run it locally once** before
trusting it fully. That's the first thing to do.

---

## 1. What is actually built and working

- **Repo structure** — matches spec section 71 exactly (`src/`, `airflow/`, `sql/`, `data/`,
  `evaluation/`, `tests/`, `docs/`, `docker/`).
- **TLC Yellow Taxi source connector** (`src/sources/tlc_taxi.py`) — discovers the monthly
  file URL (`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`,
  the real TLC CloudFront pattern), streams the download to disk in chunks (no full-file
  in-memory load), validates schema and row count via `pl.scan_parquet` (lazy, so it doesn't
  materialize the whole file just to check columns).
- **Taxi zone lookup connector** (`src/sources/taxi_zone.py`) — same pattern, CSV instead of
  Parquet, plus a uniqueness check on `LocationID`.
- **Schema contracts** (`src/contracts/*.yaml`) — separate from transform code, as required
  by section 25. Three contracts defined: trips, zones, weather (weather contract is defined
  ahead of its connector, so the target shape is explicit for milestone 2).
- **Schema validator** (`src/validation/schema_validator.py`) — checks required columns
  present, flags unexpected columns (fail-closed policy, documented in
  `docs/data/data_contracts.md`).
- **Manifest store** (`src/ingestion/manifest.py`) — JSON-file-backed for now (not yet
  Postgres-backed), keyed by dataset/period, with a `sha256 + status == "validated"` check
  that makes re-runs a no-op. This is the idempotency mechanism from section 19/21.
- **MinIO object store wrapper** (`src/storage/object_store.py`) — bucket creation, upload,
  existence check, deterministic raw key layout: `raw/<dataset>/period=<yyyy-mm>/<file>`.
- **Orchestration script** (`scripts/run_milestone1.py`) — wires discover → download →
  checksum → manifest-check → validate → upload → manifest-write. This is the runnable
  milestone-1 checkpoint from section 82, steps 9–15.
- **Airflow DAG** (`airflow/dags/quanta_flow_monthly.py`) — TaskFlow API, parameterized by a
  `period` param, two real tasks (`discover_sources`, `download_and_register`) that call into
  the same code the standalone script uses. No large data goes through XCom (section 37).
- **SQL DDL** — `sql/monitoring/create_tables.sql` (pipeline_runs, file_manifest,
  quality_results) and `sql/warehouse/create_tables.sql` (the four gold tables) are written
  and syntactically complete, mounted into Postgres init via `docker-compose.yml`, but **no
  code loads data into them yet** — the manifest is still file-based, not Postgres-based.
- **Docker Compose stack** — Postgres, MinIO, Airflow (standalone/LocalExecutor, single
  container — deliberately not split into webserver/scheduler/worker, since this is a minor
  project per section 66, not a production HA deployment).
- **Tests** — unit tests for hashing and manifest idempotency, contract tests for the schema
  validator, an integration test that builds a small local Parquet fixture and validates it
  through the real `TLCTaxiSource.validate()` path (positive and negative case), and an e2e
  test proving a second run against an already-validated manifest is skipped. This is the
  mandatory idempotency test from section 64, just not yet run against live infra.
- **CI workflow** (`.github/workflows/ci.yml`) — lint, type-check, unit/contract/integration
  tests, DAG syntax check. Not yet triggered against an actual GitHub repo.
- **Docs** — architecture, data dictionary, data contracts, operations runbook, and one ADR
  recording why milestone 1 scope was chosen this way.

## 2. What is deliberately stubbed, not built

- **NOAA weather connector** (`src/sources/noaa_weather.py`) — raises `NotImplementedError`
  on every method. The GHCNh format (PSV/Parquet, station selection, field mapping) needs
  real inspection of a downloaded file before the connector can be written correctly; I did
  not want to guess field names and ship something that looks done but silently mis-maps
  columns.
- **Bronze / silver / gold transformation layer** (`src/transformation/`) — empty package,
  one-line docstring only. No Polars lazy pipelines, no DuckDB queries, no partitioned
  Parquet writers yet.
- **Warehouse loader** (`src/warehouse/`) — empty. Nothing writes to
  `warehouse.hourly_trip_metrics` etc. yet, even though the DDL exists.
- **Monitoring** (`src/monitoring/`) — empty. `monitoring.pipeline_runs` and
  `monitoring.quality_results` tables exist in SQL but nothing writes to them; run
  observability currently only exists as structured log lines from `structlog`.
- **Manifest is file-based, not Postgres-based** — section 52 wants
  `monitoring.file_manifest` in Postgres. Milestone 1 uses local JSON so the ingestion logic
  could be built and tested without a live Postgres connection in this environment. Porting
  `ManifestStore` to write through to Postgres (while keeping the same `exists()`/`save()`
  interface) is a milestone 2 task, not a redesign.
- **Data quality framework** (section 44–46) — not started. No completeness/validity/
  uniqueness/consistency/freshness/volume checks beyond the basic schema contract check.
- **Quarantine area** (section 42) — not implemented. No bad-record routing yet, since there's
  no transformation layer producing row-level rejects yet.
- **Benchmarks** (section 48–49) — `evaluation/benchmarks/` and `evaluation/results/` exist
  as empty directories with `.gitkeep`. No CSV-vs-Parquet, eager-vs-lazy, or filtered-scan
  benchmark scripts written yet.
- **Notebooks** (section 73) — none created yet.
- **Backfill via Airflow** — currently a documented shell loop in
  `docs/operations/pipeline_operations.md`, not native Airflow backfill, because the DAG only
  has two tasks so far and backfill is more meaningful once bronze/silver/gold exist.

## 3. What you need to do on your local desk before trusting this

1. `pip install -e ".[dev]"` in a real environment with network access — this sandbox could
   not install `polars`/`duckdb`/etc. to actually execute anything, only `py_compile` to check
   syntax. Run the full test suite for real: `pytest tests/unit tests/contracts
   tests/integration tests/e2e -v`.
2. `docker compose up -d`, confirm Postgres and MinIO come up healthy, confirm
   `sql/monitoring/create_tables.sql` and `sql/warehouse/create_tables.sql` actually apply
   cleanly via the Postgres init-scripts mount (I could not verify this without a running
   Postgres container).
3. Run `python scripts/run_milestone1.py --period 2024-01` against the real TLC URL and
   confirm: the file downloads, the row count printed is sane for a real month of NYC yellow
   taxi data (millions of rows, not the 10-row fixture the tests use), and the manifest JSON
   lands in `data/manifests/nyc_tlc_yellow/2024-01.json`.
4. Run it a second time with the same period and confirm you see
   `skip_already_processed` in the logs, not a re-download.
5. Load the DAG into an actual Airflow 3.x instance (`docker compose up -d` brings one up)
   and confirm it parses and the two tasks run in order.

## 4. Recommended next-session order (per the original plan)

1. Port `ManifestStore` to Postgres (`monitoring.file_manifest`), keep the same interface.
2. Implement the NOAA GHCNh connector — after inspecting one real downloaded file's actual
   column names/types, since the spec explicitly says not to hard-code the schema before
   inspection (section 7, same principle applies to weather).
3. Build bronze normalization for all three sources (typed columns, source metadata columns,
   no business logic yet — section 14).
4. Build silver: dedup, null handling, timestamp normalization to UTC, the
   `silver.trips` / `silver.weather` / `silver.zones` / `silver.trip_weather` join
   (document the pickup-hour-to-weather-hour bucketing rule explicitly, per section 29).
5. Build the four gold aggregates and the warehouse loader.
6. Add `monitoring.pipeline_runs` and `monitoring.quality_results` writes.
7. Add the data quality framework (completeness/validity/uniqueness/consistency/freshness/
   volume) as its own module, machine-readable JSON output per section 44.
8. Add quarantine routing once silver-layer row-level rejects exist.
9. Extend the Airflow DAG with `build_bronze` → `build_gold` → `load_postgres` →
   `run_quality_checks`, backfill across 2024-01 through 2024-06.
10. Write the three benchmark scripts and populate `evaluation/pipeline_benchmarks.csv`.
11. Write the three notebooks (source inspection, quality exploration, benchmark) — last,
    since they're exploratory/demonstrative, not pipeline logic.

## 5. Known gaps / things to double-check, not just "TODO"

- The TLC and taxi-zone CloudFront URLs in the connectors are the real, current public URL
  pattern as of my knowledge cutoff (Jan 2026) — verify they still resolve; TLC has changed
  its hosting before.
- `schema_hash` in the manifest is a fingerprint of column name+type, not a full schema
  contract; a genuine type change will change the hash but nothing currently *fails* the
  pipeline on that basis (section 43 wants a hard failure on changed type) — that logic still
  needs to be written into the validator, it currently only fails on missing/unexpected
  columns.
- `docker-compose.yml` runs Airflow as a single `standalone` container for simplicity; if you
  want closer parity with the spec's webserver/scheduler/worker split (section 66), that's a
  compose change, not a code change.
