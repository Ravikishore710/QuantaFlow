from datetime import datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.validation.ingress import (
    BackfillExecutionRequest,
    PipelineExecutionRequest,
    QuarantineReplayRequest,
    TaxiTripIngressSchema,
    WeatherObservationIngressSchema,
)


def test_pipeline_execution_request_valid() -> None:
    req = PipelineExecutionRequest(
        period="2024-05",
        engine_mode="duckdb",
        skip_upload=True,
        max_quarantine_rate=0.10,
    )
    assert req.period == "2024-05"
    assert req.engine_mode == "duckdb"
    assert req.skip_upload is True


def test_pipeline_execution_request_invalid_period() -> None:
    # Invalid month 13
    with pytest.raises(ValidationError):
        PipelineExecutionRequest(period="2024-13")

    # Invalid format
    with pytest.raises(ValidationError):
        PipelineExecutionRequest(period="2024/01")

    # Extra unknown field forbidden
    with pytest.raises(ValidationError):
        PipelineExecutionRequest(period="2024-01", unexpected_field="malicious")  # type: ignore[call-arg]


def test_backfill_execution_request_chronology() -> None:
    # Valid ordering
    req = BackfillExecutionRequest(start_period="2024-01", end_period="2024-06")
    assert req.start_period == "2024-01"
    assert req.end_period == "2024-06"

    # Inverted chronology fails
    with pytest.raises(ValidationError) as exc_info:
        BackfillExecutionRequest(start_period="2024-06", end_period="2024-01")
    assert "must be <= end_period" in str(exc_info.value)


def test_quarantine_replay_request_path_traversal() -> None:
    # Valid path
    safe_req = QuarantineReplayRequest(
        period="2024-01", quarantine_dir=Path("data/quarantine"), dry_run=True
    )
    assert safe_req.dry_run is True

    # Path traversal attempt rejected
    with pytest.raises(ValidationError) as exc_info:
        QuarantineReplayRequest(quarantine_dir=Path("../../etc/passwd"))
    assert "Path traversal characters detected" in str(exc_info.value)


def test_taxi_trip_ingress_schema_valid_and_bounds() -> None:
    valid_record = {
        "tpep_pickup_datetime": datetime(2024, 1, 1, 12, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 12, 20, 0),
        "PULocationID": 161,
        "DOLocationID": 230,
        "trip_distance": 2.5,
        "fare_amount": 14.5,
        "total_amount": 18.0,
        "passenger_count": 2,
    }
    schema = TaxiTripIngressSchema(**valid_record)
    assert schema.fare_amount == 14.5

    # Negative distance rejected
    with pytest.raises(ValidationError):
        TaxiTripIngressSchema(**{**valid_record, "trip_distance": -1.0})

    # Dropoff before pickup rejected
    with pytest.raises(ValidationError):
        TaxiTripIngressSchema(
            **{
                **valid_record,
                "tpep_dropoff_datetime": datetime(2024, 1, 1, 11, 0, 0),
            }
        )

    # Fare exceeds total rejected
    with pytest.raises(ValidationError):
        TaxiTripIngressSchema(**{**valid_record, "fare_amount": 25.0, "total_amount": 20.0})


def test_weather_observation_ingress_schema() -> None:
    obs = {
        "timestamp": datetime(2024, 1, 1, 12, 0, 0),
        "station_id": "USW00094728",
        "temperature": 5.4,
        "wind_speed": 4.1,
        "precipitation": 0.0,
    }
    schema = WeatherObservationIngressSchema(**obs)
    assert schema.station_id == "USW00094728"

    # Extreme unrealistic temperature rejected
    with pytest.raises(ValidationError):
        WeatherObservationIngressSchema(**{**obs, "temperature": 150.0})
