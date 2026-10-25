from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.common.errors import DataQualityGateError
from src.validation.quality_framework import (
    DataQualityFramework,
    QualityCheckResult,
    QualitySuiteReport,
    send_quality_alert,
)


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


def test_wap_gate_passes_healthy_report(tmp_path: Path) -> None:
    dq = DataQualityFramework()
    report = QualitySuiteReport(
        run_id="run-pass",
        dataset="nyc_tlc_yellow",
        partition="2024-01",
        evaluated_at="2024-01-01T00:00:00Z",
        overall_status="passed",
        total_checks=6,
        passed_checks=6,
        failed_checks=0,
        checks=[
            QualityCheckResult(
                check_name="c1",
                dimension="completeness",
                severity="error",
                rows_checked=10,
                violations=0,
                status="passed",
                details={},
            )
        ],
    )
    # Should not raise
    dq.enforce_wap_gate(report)


def test_wap_gate_trips_on_error_check() -> None:
    dq = DataQualityFramework()
    report = QualitySuiteReport(
        run_id="run-fail-error",
        dataset="nyc_tlc_yellow",
        partition="2024-01",
        evaluated_at="2024-01-01T00:00:00Z",
        overall_status="failed",
        total_checks=6,
        passed_checks=5,
        failed_checks=1,
        checks=[
            QualityCheckResult(
                check_name="critical_completeness",
                dimension="completeness",
                severity="error",
                rows_checked=10,
                violations=2,
                status="failed",
                details={},
            )
        ],
    )
    with pytest.raises(DataQualityGateError) as exc_info:
        dq.enforce_wap_gate(report)

    assert "critical checks failed" in str(exc_info.value)
    assert "critical_completeness" in exc_info.value.failed_checks


def test_wap_gate_trips_on_low_score() -> None:
    dq = DataQualityFramework()
    report = QualitySuiteReport(
        run_id="run-fail-score",
        dataset="nyc_tlc_yellow",
        partition="2024-01",
        evaluated_at="2024-01-01T00:00:00Z",
        overall_status="passed",
        total_checks=10,
        passed_checks=7,
        failed_checks=3,
        checks=[
            QualityCheckResult(
                check_name=f"warn_{i}",
                dimension="warning_dim",
                severity="warning",
                rows_checked=10,
                violations=1,
                status="failed",
                details={},
            )
            for i in range(3)
        ],
    )
    with pytest.raises(DataQualityGateError) as exc_info:
        dq.enforce_wap_gate(report, min_score=0.85)

    assert "score 0.70 < threshold 0.85" in str(exc_info.value)
    assert len(exc_info.value.failed_checks) == 3


def test_wap_gate_supports_dict_report() -> None:
    dq = DataQualityFramework()
    healthy_dict = {
        "run_id": "run-dict",
        "dataset": "test",
        "overall_status": "passed",
        "total_checks": 5,
        "passed_checks": 5,
        "failed_checks": 0,
        "checks": [],
    }
    dq.enforce_wap_gate(healthy_dict)

    failing_dict = {
        "run_id": "run-dict-fail",
        "dataset": "test",
        "overall_status": "failed",
        "total_checks": 5,
        "passed_checks": 4,
        "failed_checks": 1,
        "checks": [{"check_name": "crit_chk", "status": "failed", "severity": "error"}],
    }
    with pytest.raises(DataQualityGateError):
        dq.enforce_wap_gate(failing_dict)


def test_send_quality_alert_success() -> None:
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        success = send_quality_alert({"alert": "test"}, webhook_url="https://hooks.slack.com/test")
        assert success is True
        mock_urlopen.assert_called_once()


def test_send_quality_alert_empty_url() -> None:
    assert send_quality_alert({"alert": "test"}, webhook_url="") is False
    assert send_quality_alert({"alert": "test"}, webhook_url=None) is False


def test_send_quality_alert_handles_exception() -> None:
    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        success = send_quality_alert({"alert": "test"}, webhook_url="https://hooks.slack.com/test")
        assert success is False
