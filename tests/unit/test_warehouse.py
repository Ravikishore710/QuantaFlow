from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.common.errors import WarehouseConnectionError
from src.warehouse.loader import WarehouseLoader


@pytest.fixture
def sample_hourly_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "hourly.parquet"
    df = pl.DataFrame(
        {
            "trip_hour": [datetime(2024, 1, 1, 12, 0)],
            "trip_count": [100],
            "total_distance": [250.5],
            "total_fare": [1500.0],
            "avg_fare": [15.0],
            "avg_distance": [2.5],
            "avg_trip_duration": [600.0],
            "avg_passenger_count": [1.5],
            "source_period": ["2024-01"],
        }
    )
    df.write_parquet(p)
    return p


@pytest.fixture
def sample_zone_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "zone.parquet"
    df = pl.DataFrame(
        {
            "trip_date": [date(2024, 1, 1)],
            "location_id": [161],
            "trip_count": [50],
            "total_fare": [750.0],
            "avg_distance": [3.0],
            "avg_duration": [720.0],
            "source_period": ["2024-01"],
        }
    )
    df.write_parquet(p)
    return p


@pytest.fixture
def sample_weather_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "weather.parquet"
    df = pl.DataFrame(
        {
            "weather_hour": [datetime(2024, 1, 1, 12, 0)],
            "trip_count": [100],
            "avg_trip_distance": [2.5],
            "avg_trip_duration": [600.0],
            "avg_total_amount": [18.5],
            "precipitation": [0.0],
            "temperature": [5.2],
            "wind_speed": [3.1],
            "source_period": ["2024-01"],
        }
    )
    df.write_parquet(p)
    return p


@pytest.fixture
def sample_system_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "system.parquet"
    df = pl.DataFrame(
        {
            "run_date": [date(2024, 1, 1)],
            "rows_ingested": [1000],
            "rows_valid": [980],
            "rows_rejected": [20],
            "files_processed": [3],
            "processing_time_seconds": [12.5],
        }
    )
    df.write_parquet(p)
    return p


def test_duckdb_loading_all_marts(
    tmp_path: Path,
    sample_hourly_parquet: Path,
    sample_zone_parquet: Path,
    sample_weather_parquet: Path,
    sample_system_parquet: Path,
) -> None:
    db_file = tmp_path / "test_quantaflow.duckdb"
    loader = WarehouseLoader(engine_mode="duckdb", duckdb_path=db_file)

    c1 = loader.load_hourly_trip_metrics(sample_hourly_parquet)
    c2 = loader.load_zone_daily_metrics(sample_zone_parquet)
    c3 = loader.load_weather_impact_metrics(sample_weather_parquet)
    c4 = loader.load_daily_system_metrics(sample_system_parquet)

    assert c1 == 1
    assert c2 == 1
    assert c3 == 1
    assert c4 == 1

    # Idempotent re-run
    c1_again = loader.load_hourly_trip_metrics(sample_hourly_parquet)
    assert c1_again == 1


def test_postgres_strict_mode_raises_when_unreachable(sample_hourly_parquet: Path) -> None:
    loader = WarehouseLoader(engine_mode="postgres")
    with (
        patch("psycopg2.connect", side_effect=ConnectionRefusedError("Connection refused")),
        pytest.raises(WarehouseConnectionError, match="PostgreSQL production warehouse connection failed"),
    ):
        loader.load_hourly_trip_metrics(sample_hourly_parquet)


def test_postgres_loader_executes_upsert_queries(
    tmp_path: Path,
    sample_hourly_parquet: Path,
    sample_zone_parquet: Path,
) -> None:
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    loader = WarehouseLoader(engine_mode="postgres")

    with patch.object(loader, "_get_postgres_conn", return_value=mock_conn):
        loader.load_hourly_trip_metrics(sample_hourly_parquet)
        assert mock_cursor.execute.called
        query = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO warehouse.hourly_trip_metrics" in query
        assert "ON CONFLICT (trip_hour) DO UPDATE" in query
        assert mock_conn.commit.called

    mock_cursor.reset_mock()
    mock_conn.reset_mock()

    with patch.object(loader, "_get_postgres_conn", return_value=mock_conn):
        loader.load_zone_daily_metrics(sample_zone_parquet)
        assert mock_cursor.execute.called
        query = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO warehouse.zone_daily_metrics" in query
        assert "ON CONFLICT (trip_date, location_id) DO UPDATE" in query
        assert mock_conn.commit.called
