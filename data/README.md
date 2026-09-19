# Data Directory

This directory is a local working area for the pipeline. It is not meant to hold the full
six-month dataset in Git.

## manifests/

One JSON manifest per dataset per period, written by `src/ingestion/manifest.py`. Used for
idempotency checks and provenance. Real manifests are ignored by Git; only `.gitkeep` is tracked.

## samples/

Small local fixture files used by tests and manual runs. Full downloaded source files also land
here by default when running `scripts/run_milestone1.py`, then get uploaded to MinIO. Large
files are excluded from Git via `.gitignore`.
