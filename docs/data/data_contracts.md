# Data Contracts

Schema contracts live under `src/contracts/` as YAML files, separate from transformation
code, and are loaded by `src/validation/schema_validator.py`.

## Contract shape

```yaml
dataset: <name>
version: <int>
columns:
  - name: <column name>
    required: true|false
    nullable: true|false
    validation_rule: "<optional rule, e.g. >= 0 or unique>"
    description: <text>
```

## Enforcement policy

- **Missing required column** -> validation fails, file is not promoted past raw, manifest
  status is recorded as `validation_failed`.
- **Unexpected column** -> currently flagged as a validation error (fail closed). This is the
  conservative default described in the schema evolution policy; a warn-only mode for
  additive columns is a milestone 2 configuration item.
- **Changed type** -> not yet automatically detected; `schema_hash` in the manifest captures
  column name and type fingerprints so a type change produces a different hash and is
  visible during manifest review. Automated type-change failure is planned for milestone 2.

## Contracts currently defined

- `tlc_trip_schema.yaml` — TLC Yellow Taxi trip records
- `taxi_zone_schema.yaml` — TLC taxi zone lookup
- `weather_schema.yaml` — NOAA GHCNh hourly observations (contract defined ahead of the
  connector implementation so the target shape is explicit)
