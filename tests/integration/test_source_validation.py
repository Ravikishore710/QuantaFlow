from pathlib import Path

import polars as pl
import pytest

from src.sources.base import DiscoveredFile, DownloadedFile
from src.sources.noaa_weather import NOAAWeatherSource
from src.sources.taxi_zone import TaxiZoneSource
from src.sources.tlc_taxi import TLCTaxiSource


def build_sample_parquet(path: Path, row_count: int = 10) -> None:
    frame = pl.DataFrame(
        {
            "tpep_pickup_datetime": ["2024-01-01T00:00:00"] * row_count,
            "tpep_dropoff_datetime": ["2024-01-01T00:15:00"] * row_count,
            "PULocationID": list(range(1, row_count + 1)),
            "DOLocationID": list(range(1, row_count + 1)),
            "trip_distance": [1.5] * row_count,
            "passenger_count": [1] * row_count,
            "fare_amount": [10.0] * row_count,
            "total_amount": [12.5] * row_count,
        }
    )
    frame.write_parquet(path)


def test_tlc_validate_passes_for_conformant_sample(tmp_path: Path) -> None:
    sample_path = tmp_path / "yellow_trip_2024_01.parquet"
    build_sample_parquet(sample_path)

    source = TLCTaxiSource()
    discovered = DiscoveredFile(
        dataset="nyc_tlc_yellow", period="2024-01", source_url="local", source_format="parquet"
    )
    downloaded = DownloadedFile(
        discovered=discovered, local_path=sample_path, file_size_bytes=sample_path.stat().st_size
    )

    result = source.validate(downloaded)

    assert result.passed is True
    assert result.row_count == 10
    assert "trip_distance" in result.column_names


def test_tlc_validate_fails_when_required_column_missing(tmp_path: Path) -> None:
    sample_path = tmp_path / "yellow_trip_2024_02.parquet"
    frame = pl.DataFrame({"tpep_pickup_datetime": ["2024-02-01T00:00:00"]})
    frame.write_parquet(sample_path)

    source = TLCTaxiSource()
    discovered = DiscoveredFile(
        dataset="nyc_tlc_yellow", period="2024-02", source_url="local", source_format="parquet"
    )
    downloaded = DownloadedFile(
        discovered=discovered, local_path=sample_path, file_size_bytes=sample_path.stat().st_size
    )

    result = source.validate(downloaded)

    assert result.passed is False
    assert any("missing required columns" in error for error in result.errors)


def test_offline_fixtures_resolution_with_zero_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixtures_dir = Path(__file__).resolve().parent.parent / "fixtures"
    assert fixtures_dir.exists()

    monkeypatch.setenv("QUANTAFLOW_OFFLINE_FIXTURES_DIR", str(fixtures_dir))

    # Block any network calls
    def fail_on_network(*args: object, **kwargs: object) -> None:
        raise RuntimeError("External network request forbidden during offline test!")

    monkeypatch.setattr("requests.get", fail_on_network)

    # 1. TLC Taxi offline download & validation
    tlc_src = TLCTaxiSource()
    tlc_disc = tlc_src.discover("2024-01")
    tlc_down = tlc_src.download(tlc_disc, tmp_path / "tlc")
    assert tlc_down.local_path.exists()
    tlc_val = tlc_src.validate(tlc_down)
    assert tlc_val.passed is True
    assert tlc_val.row_count == 50

    # 2. Taxi Zone offline download & validation
    zone_src = TaxiZoneSource()
    zone_disc = zone_src.discover("2024-01")
    zone_down = zone_src.download(zone_disc, tmp_path / "zones")
    assert zone_down.local_path.exists()
    zone_val = zone_src.validate(zone_down)
    assert zone_val.passed is True
    assert zone_val.row_count == 20

    # 3. NOAA Weather offline download & validation
    weather_src = NOAAWeatherSource()
    weather_disc = weather_src.discover("2024-01")
    weather_down = weather_src.download(weather_disc, tmp_path / "weather")
    assert weather_down.local_path.exists()
    weather_val = weather_src.validate(weather_down)
    assert weather_val.passed is True
    assert weather_val.row_count == 48
