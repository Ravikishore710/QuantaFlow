"""Unit tests verifying global offline test isolation and fixture resolution."""

import socket
from pathlib import Path

import pytest

from src.sources.noaa_weather import NOAAWeatherSource
from src.sources.taxi_zone import TaxiZoneSource
from src.sources.tlc_taxi import TLCTaxiSource


def test_offline_fixture_resolution_tlc_taxi(tmp_path: Path) -> None:
    """Verify TLCTaxiSource resolves from offline fixtures without network calls."""
    source = TLCTaxiSource()
    discovered = source.discover(period="2024-01")
    downloaded = source.download(discovered, destination_dir=tmp_path / "tlc")

    assert downloaded.local_path.exists()
    assert downloaded.file_size_bytes > 0
    validation = source.validate(downloaded)
    assert validation.passed is True
    assert validation.row_count > 0


def test_offline_fixture_resolution_noaa_weather(tmp_path: Path) -> None:
    """Verify NOAAWeatherSource resolves from offline fixtures without network calls."""
    source = NOAAWeatherSource(station_id="USW00094728")
    discovered = source.discover(period="2024-01")
    downloaded = source.download(discovered, destination_dir=tmp_path / "noaa")

    assert downloaded.local_path.exists()
    assert downloaded.file_size_bytes > 0
    validation = source.validate(downloaded)
    assert validation.passed is True
    assert validation.row_count > 0


def test_offline_fixture_resolution_taxi_zone(tmp_path: Path) -> None:
    """Verify TaxiZoneSource resolves from offline fixtures without network calls."""
    source = TaxiZoneSource()
    discovered = source.discover(period="2024-01")
    downloaded = source.download(discovered, destination_dir=tmp_path / "zones")

    assert downloaded.local_path.exists()
    assert downloaded.file_size_bytes > 0
    validation = source.validate(downloaded)
    assert validation.passed is True
    assert validation.row_count > 0


def test_external_network_call_blocked() -> None:
    """Verify that socket attempts to external domains are intercepted and blocked."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        with pytest.raises(RuntimeError, match="External network call blocked"):
            sock.connect(("8.8.8.8", 53))
    finally:
        sock.close()
