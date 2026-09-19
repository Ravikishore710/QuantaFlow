# QuantaFlow Architecture

## Layer flow

```
Source systems (TLC, NOAA GHCNh)
  -> Ingestion (download, checksum, manifest)
  -> Raw (immutable copy in MinIO, object key: raw/<dataset>/period=<yyyy-mm>/<file>)
  -> Bronze (schema-normalized, typed, source metadata columns added)
  -> Silver (deduplicated, cleaned, joined across sources)
  -> Gold (business-ready aggregates, one row per analytical grain)
  -> PostgreSQL warehouse schema (curated serving layer)
```

## Current implementation status

Milestone 1 covers the path from source discovery through raw storage and manifest
registration for the TLC Yellow Taxi source only. Bronze, silver and gold layers are not
implemented yet; see `HANDOVER.md` for the detailed next-steps plan.

## Component responsibilities

- **Polars (lazy API)**: primary transformation engine once bronze/silver/gold are built.
  `pl.scan_parquet` is used instead of eager reads so files larger than memory stay
  streamable.
- **DuckDB**: file-level SQL, validation queries and cross-file analytics against Parquet
  in MinIO, used alongside Polars rather than instead of it.
- **MinIO**: local S3-compatible object store standing in for cloud object storage. Object
  keys are partitioned by dataset and period so the same layout maps directly onto S3 later.
- **PostgreSQL**: two logical schemas, `warehouse` for gold-layer analytical tables and
  `monitoring` for pipeline run history, file manifests and quality check results.
- **Airflow (TaskFlow API)**: orchestrates the DAG `quanta_flow_monthly_pipeline`,
  parameterized by a `period` param in `YYYY-MM` format. Tasks pass file paths, manifest
  identifiers and small metadata through XCom only, never full datasets.

## Idempotency model

Every downloaded file is hashed with SHA-256. Before processing, the manifest store is
checked for an existing record with a matching dataset, period and hash whose status is
`validated`. A match short-circuits the run. This is what makes re-running a period, or
re-running the whole DAG, safe.

## Partitioning

Trip data partitions by `year=YYYY/month=MM` in the silver and gold layers once those layers
exist, matching the monthly cadence of the source files. Object storage raw paths partition
by `period=YYYY-MM` from milestone 1 onward.
