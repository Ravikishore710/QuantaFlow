from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl

from src.sources.base import DownloadedFile
from src.sources.noaa_weather import NOAAWeatherSource
from src.sources.synthetic import generate_synthetic_taxi_data, generate_synthetic_weather_data
from src.sources.taxi_zone import TaxiZoneSource
from src.sources.tlc_taxi import TLCTaxiSource


def test_generate_synthetic_taxi_data(tmp_path: Path) -> None:
    target = tmp_path / "taxi.parquet"
    out = generate_synthetic_taxi_data(period="2024-01", num_rows=10, target_path=target)
    assert out.exists()
    df = pl.read_parquet(out)
    assert len(df) == 10
    assert "tpep_pickup_datetime" in df.columns
    assert "total_amount" in df.columns


def test_generate_synthetic_weather_data(tmp_path: Path) -> None:
    target = tmp_path / "weather.parquet"
    out = generate_synthetic_weather_data(
        period="2024-01", station_id="USW00094728", target_path=target
    )
    assert out.exists()
    df = pl.read_parquet(out)
    assert len(df) == 744  # January has 31 days * 24 hours
    assert "temperature" in df.columns
    assert "station_id" in df.columns


def test_taxi_zone_source(tmp_path: Path) -> None:
    source = TaxiZoneSource()
    discovered = source.discover(period="2024-01")
    assert discovered.dataset == "nyc_tlc_taxi_zone"
    assert discovered.source_format == "csv"

    # Test download with existing file
    local_csv = tmp_path / "taxi_zone_lookup.csv"
    csv_content = (
        "LocationID,Borough,Zone,service_zone\n"
        "1,Manhattan,Central Park,Yellow Zone\n"
        "2,Queens,JFK Airport,Airports\n"
    )
    local_csv.write_text(csv_content)

    downloaded = source.download(discovered, tmp_path)
    assert downloaded.local_path == local_csv
    assert downloaded.file_size_bytes > 0

    # Test validate passes
    validation = source.validate(downloaded)
    assert validation.passed is True
    assert validation.row_count == 2

    # Test validate fails on non-unique LocationID
    bad_csv = tmp_path / "bad.csv"
    bad_content = (
        "LocationID,Borough,Zone,service_zone\n"
        "1,Manhattan,Central Park,Yellow Zone\n"
        "1,Queens,JFK Airport,Airports\n"
    )
    bad_csv.write_text(bad_content)
    bad_downloaded = DownloadedFile(
        discovered=discovered, local_path=bad_csv, file_size_bytes=100
    )
    bad_validation = source.validate(bad_downloaded)
    assert bad_validation.passed is False
    assert any("not unique" in err for err in bad_validation.errors)


def test_taxi_zone_download_via_requests(tmp_path: Path) -> None:
    source = TaxiZoneSource()
    discovered = source.discover(period="2024-01")

    dest_dir = tmp_path / "fresh"
    mock_resp = MagicMock()
    mock_resp.content = (
        b"LocationID,Borough,Zone,service_zone\n"
        b"1,Manhattan,Central Park,Yellow Zone\n"
    )
    mock_resp.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_resp):
        downloaded = source.download(discovered, dest_dir)
        assert downloaded.local_path.exists()
        assert downloaded.file_size_bytes == len(mock_resp.content)


def test_tlc_taxi_source(tmp_path: Path) -> None:
    source = TLCTaxiSource()
    discovered = source.discover("2024-01")
    assert discovered.dataset == "nyc_tlc_yellow"
    assert "yellow_tripdata_2024-01.parquet" in discovered.source_url

    # Existing local file
    dest_dir = tmp_path / "tlc"
    dest_dir.mkdir(parents=True)
    local_pq = dest_dir / "yellow_trip_2024_01.parquet"
    pl.DataFrame({"tpep_pickup_datetime": [datetime(2024, 1, 1)]}).write_parquet(local_pq)

    downloaded = source.download(discovered, dest_dir)
    assert downloaded.local_path == local_pq

    # Mock requests download
    dest_dir2 = tmp_path / "fresh_tlc"
    mock_resp = MagicMock()
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.iter_content = MagicMock(return_value=[b"PAR1", b"data"])
    mock_resp.raise_for_status = MagicMock()
    with patch("requests.get", return_value=mock_resp):
        dl2 = source.download(discovered, dest_dir2)
        assert dl2.local_path.exists()


def test_noaa_weather_source(tmp_path: Path) -> None:
    source = NOAAWeatherSource(station_id="USW00094728")
    discovered = source.discover("2024-01")
    assert discovered.dataset == "noaa_ghcnh"
    assert "2024" in discovered.source_url

    dest_dir = tmp_path / "weather"
    dest_dir.mkdir(parents=True)
    local_pq = dest_dir / "ghcnh_USW00094728_2024_01.parquet"

    # Pre-generate sample
    generate_synthetic_weather_data(
        "2024-01", station_id="USW00094728", target_path=local_pq
    )

    downloaded = source.download(discovered, dest_dir)
    assert downloaded.local_path == local_pq

    validation = source.validate(downloaded)
    assert validation.passed is True
    assert validation.row_count == 744


def test_noaa_weather_download_via_requests(tmp_path: Path) -> None:
    source = NOAAWeatherSource(station_id="USW00094728")
    discovered = source.discover("2024-01")

    # Create in-memory parquet data with Year, Month, STATION, and weather fields
    sample_data = pl.DataFrame({
        "Year": ["2024", "2024", "2023"],
        "Month": ["01", "01", "12"],
        "STATION": ["USW00094728", "USW00094728", "USW00094728"],
        "LATITUDE": [40.78, 40.78, 40.78],
        "LONGITUDE": [-73.97, -73.97, -73.97],
        "DATE": [
            "2024-01-01T00:00:00",
            "2024-01-01T01:00:00",
            "2023-12-31T23:00:00",
        ],
        "temperature": [2.5, 3.1, -1.0],
        "dew_point_temperature": [-1.2, -0.8, -3.0],
        "wind_speed": [5.2, 6.0, 4.0],
        "precipitation": [0.0, 0.5, 0.0],
        "visibility": [16000.0, 15000.0, 10000.0],
        "relative_humidity": [75.0, 80.0, 70.0],
        "wind_direction": [180.0, 190.0, 200.0],
    })
    import io
    buf = io.BytesIO()
    sample_data.write_parquet(buf)

    mock_resp = MagicMock()
    mock_resp.content = buf.getvalue()
    mock_resp.raise_for_status = MagicMock()

    dest_dir = tmp_path / "fresh_noaa"
    with patch("requests.get", return_value=mock_resp):
        downloaded = source.download(discovered, dest_dir)
        assert downloaded.local_path.exists()
        df = pl.read_parquet(downloaded.local_path)
        assert len(df) == 2
        assert "timestamp" in df.columns
        assert "dew_point" in df.columns

    # Test validation failure on empty parquet
    empty_pq = tmp_path / "empty.parquet"
    pl.DataFrame({"timestamp": [], "station_id": []}).write_parquet(empty_pq)
    empty_downloaded = DownloadedFile(
        discovered=discovered, local_path=empty_pq, file_size_bytes=10
    )
    val = source.validate(empty_downloaded)
    assert val.passed is False


def test_noaa_weather_download_date_filter(tmp_path: Path) -> None:
    source = NOAAWeatherSource(station_id="USW00094728")
    discovered = source.discover("2024-01")

    # DataFrame with DATE and direct column names without Year/Month
    sample_data = pl.DataFrame({
        "DATE": ["2024-01-01T00:00:00", "2024-02-01T00:00:00"],
        "station_id": ["USW00094728", "USW00094728"],
        "latitude": [40.78, 40.78],
        "longitude": [-73.97, -73.97],
        "temperature": [2.5, 4.0],
        "dew_point": [-1.2, -0.5],
        "wind_speed": [5.2, 3.1],
        "precipitation": [0.0, 0.0],
        "visibility": [16000.0, 16000.0],
        "relative_humidity": [75.0, 70.0],
        "wind_direction": [180.0, 180.0],
    })
    import io
    buf = io.BytesIO()
    sample_data.write_parquet(buf)

    mock_resp = MagicMock()
    mock_resp.content = buf.getvalue()
    mock_resp.raise_for_status = MagicMock()

    dest_dir = tmp_path / "date_filtered_noaa"
    with patch("requests.get", return_value=mock_resp):
        downloaded = source.download(discovered, dest_dir)
        df = pl.read_parquet(downloaded.local_path)
        assert len(df) == 1

