from pathlib import Path

from src.validation.schema_validator import load_contract, validate_columns

WEATHER_CONTRACT = Path("src/contracts/weather_schema.yaml")


def test_weather_contract_dataset_name() -> None:
    contract = load_contract(WEATHER_CONTRACT)
    assert contract["dataset"] == "noaa_ghcnh"
    required_cols = [c["name"] for c in contract["columns"] if c.get("required")]
    assert "timestamp" in required_cols
    assert "station_id" in required_cols
    assert "temperature" in required_cols


def test_weather_columns_validation_passes() -> None:
    contract = load_contract(WEATHER_CONTRACT)
    actual = [
        "timestamp",
        "station_id",
        "latitude",
        "longitude",
        "temperature",
        "dew_point",
        "wind_speed",
        "precipitation",
        "visibility",
        "pressure",
        "weather_condition",
    ]
    errors = validate_columns(actual, contract, allow_unexpected=False)
    assert errors == []
