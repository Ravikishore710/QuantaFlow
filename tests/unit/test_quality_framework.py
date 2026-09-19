from datetime import datetime
from pathlib import Path

import polars as pl

from src.validation.quality_framework import DataQualityFramework


def test_quality_framework_evaluates_all_dimensions(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "tpep_pickup_datetime": [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 2, 11, 0, 0),
            ],
            "tpep_dropoff_datetime": [
                datetime(2024, 1, 1, 10, 15, 0),
                datetime(2024, 1, 2, 11, 25, 0),
            ],
            "PULocationID": [10, 20],
            "DOLocationID": [15, 25],
            "trip_distance": [2.5, 3.8],
            "fare_amount": [12.0, 18.5],
            "total_amount": [15.0, 22.0],
            "passenger_count": [1, 2],
        }
    )

    dq = DataQualityFramework()
    dq.reports_dir = tmp_path / "reports"

    report = dq.evaluate_trips(df, run_id="test-run-123", period="2024-01", previous_row_count=2)

    assert report.overall_status == "passed"
    assert report.total_checks == 6
    assert report.failed_checks == 0
    assert (tmp_path / "reports" / "quality_report_2024-01_test-run-123.json").exists()


def test_quality_framework_detects_violations(tmp_path: Path) -> None:
    # Introduce negative fare and missing value
    df = pl.DataFrame(
        {
            "tpep_pickup_datetime": [datetime(2024, 1, 1, 10, 0, 0)],
            "tpep_dropoff_datetime": [datetime(2024, 1, 1, 10, 15, 0)],
            "PULocationID": [10],
            "DOLocationID": [15],
            "trip_distance": [2.5],
            "fare_amount": [-10.0],  # negative fare
            "total_amount": [5.0],
            "passenger_count": [1],
        }
    )

    dq = DataQualityFramework()
    dq.reports_dir = tmp_path / "reports"

    report = dq.evaluate_trips(df, run_id="test-run-fail", period="2024-01")

    assert report.overall_status == "failed"
    validity_check = next(c for c in report.checks if c.dimension == "validity")
    assert validity_check.status == "failed"
    assert validity_check.violations > 0
