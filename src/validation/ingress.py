from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictIngressModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=False, validate_assignment=True)


class PipelineExecutionRequest(StrictIngressModel):
    """Ingress schema validating pipeline execution parameters from CLI or DAG triggers."""

    period: str = Field(
        ...,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="Execution month partition in YYYY-MM format",
    )
    engine_mode: Literal["auto", "postgres", "duckdb"] = Field(
        default="auto",
        description="Database warehouse target engine",
    )
    skip_upload: bool = Field(
        default=False,
        description="Skip remote object store (MinIO/S3) upload",
    )
    use_synthetic: bool = Field(
        default=False,
        description="Use synthetic data generation for testing",
    )
    max_quarantine_rate: float = Field(
        default=0.15,
        ge=0.0,
        le=1.0,
        description="Maximum tolerable quarantine fraction before tripping circuit breaker",
    )


class BackfillExecutionRequest(StrictIngressModel):
    """Ingress schema validating historical multi-month backfill requests."""

    start_period: str = Field(
        ...,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="Start partition in YYYY-MM format",
    )
    end_period: str = Field(
        ...,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="End partition in YYYY-MM format",
    )
    use_synthetic: bool = Field(default=False)
    skip_upload: bool = Field(default=True)

    @model_validator(mode="after")
    def validate_chronology(self) -> "BackfillExecutionRequest":
        if self.start_period > self.end_period:
            raise ValueError(
                f"start_period '{self.start_period}' must be <= end_period '{self.end_period}'"
            )
        return self


class QuarantineReplayRequest(StrictIngressModel):
    """Ingress schema validating dead-letter queue replay requests."""

    period: str | None = Field(
        default=None,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="Optional period filter",
    )
    quarantine_dir: Path = Field(default=Path("data/quarantine"))
    silver_dir: Path = Field(default=Path("data/silver"))
    dry_run: bool = Field(default=False)

    @field_validator("quarantine_dir", "silver_dir")
    @classmethod
    def validate_safe_path(cls, path: Path) -> Path:
        resolved = str(path.as_posix())
        if ".." in resolved.split("/"):
            raise ValueError(f"Path traversal characters detected in path: '{path}'")
        return path


class TaxiTripIngressSchema(StrictIngressModel):
    """Row-level ingress schema for taxi trip records."""

    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: datetime
    PULocationID: int = Field(gt=0, le=300)
    DOLocationID: int = Field(gt=0, le=300)
    trip_distance: float = Field(ge=0.0, le=500.0)
    fare_amount: float = Field(ge=0.0, le=10000.0)
    total_amount: float = Field(ge=0.0)
    passenger_count: int = Field(default=1, ge=0, le=10)

    @model_validator(mode="after")
    def validate_trip_physics(self) -> "TaxiTripIngressSchema":
        if self.tpep_dropoff_datetime < self.tpep_pickup_datetime:
            raise ValueError("dropoff_datetime cannot precede pickup_datetime")
        if self.total_amount < self.fare_amount:
            raise ValueError("total_amount cannot be less than fare_amount")
        return self


class WeatherObservationIngressSchema(StrictIngressModel):
    """Row-level ingress schema for weather observations."""

    timestamp: datetime
    station_id: str = Field(min_length=3, max_length=20)
    temperature: float = Field(ge=-60.0, le=60.0)
    wind_speed: float = Field(ge=0.0, le=150.0)
    precipitation: float = Field(ge=0.0, le=500.0)
