"""Unit tests verifying MAD Z-score statistical anomaly detection for data engineering metrics."""

from datetime import datetime
from pathlib import Path

import polars as pl

from src.quality.anomaly import StatisticalAnomalyDetector
from src.validation.quality_framework import DataQualityFramework


def test_anomaly_detector_normal_fluctuations() -> None:
    """Verify normal variations around the median are not flagged as anomalies."""
    detector = StatisticalAnomalyDetector()
    baseline = [100.0, 102.0, 98.0, 105.0, 99.0, 101.0, 103.0, 97.0]

    result = detector.evaluate_metric(
        metric_name="row_count",
        observed_value=104.0,
        baseline=baseline,
        z_threshold=3.5,
    )
    assert result.is_anomaly is False
    assert result.reason == "normal"
    assert abs(result.z_score) < 3.5


def test_anomaly_detector_volume_drop() -> None:
    """Verify sudden severe volume drop (e.g., 5-sigma drop) triggers anomaly detection."""
    detector = StatisticalAnomalyDetector()
    baseline = [100000.0, 102000.0, 99000.0, 101000.0, 100500.0, 99500.0]

    result = detector.evaluate_metric(
        metric_name="row_count",
        observed_value=15000.0,
        baseline=baseline,
        z_threshold=3.5,
    )
    assert result.is_anomaly is True
    assert "drop_detected" in result.reason
    assert result.z_score < -3.5


def test_anomaly_detector_fare_spike() -> None:
    """Verify severe fare spike triggers anomaly detection."""
    detector = StatisticalAnomalyDetector()
    baseline = [15.2, 16.0, 14.8, 15.5, 15.0, 16.2]

    result = detector.evaluate_metric(
        metric_name="mean_fare",
        observed_value=120.0,
        baseline=baseline,
        z_threshold=3.5,
    )
    assert result.is_anomaly is True
    assert "spike_detected" in result.reason
    assert result.z_score > 3.5


def test_anomaly_detector_batch_evaluation() -> None:
    """Verify multi-metric batch evaluation against historical series."""
    detector = StatisticalAnomalyDetector()
    metrics = {
        "row_count": 50000.0,
        "mean_fare": 16.0,
        "mean_distance": 25.0,  # anomalous distance
    }
    baselines = {
        "row_count": [49000.0, 51000.0, 50500.0, 49500.0],
        "mean_fare": [15.5, 16.2, 15.8, 16.0],
        "mean_distance": [2.8, 3.1, 2.9, 3.0, 3.2],
    }

    results = detector.evaluate_batch(metrics, baselines, z_threshold=3.5)
    assert len(results) == 3

    by_name = {r.metric_name: r for r in results}
    assert by_name["row_count"].is_anomaly is False
    assert by_name["mean_fare"].is_anomaly is False
    assert by_name["mean_distance"].is_anomaly is True


def test_data_quality_framework_integration(tmp_path: Path) -> None:
    """Verify DataQualityFramework integrates statistical anomaly checks into suite report."""
    dq = DataQualityFramework()
    dq.reports_dir = tmp_path

    df = pl.DataFrame({
        "tpep_pickup_datetime": [datetime(2024, 1, 15, 10, 0)],
        "tpep_dropoff_datetime": [datetime(2024, 1, 15, 10, 20)],
        "PULocationID": [1],
        "DOLocationID": [2],
        "trip_distance": [2.5],
        "fare_amount": [12.0],
        "total_amount": [15.0],
    })

    # Historical baseline where 1 row is an extreme anomaly
    baselines = {
        "row_count": [10000.0, 10500.0, 9800.0, 10200.0, 10100.0],
        "mean_fare": [11.8, 12.1, 12.0, 12.2],
    }

    report = dq.evaluate_trips(
        df=df,
        run_id="run-test-anomaly",
        period="2024-01",
        historical_baselines=baselines,
    )
    assert report.total_checks == 7

    anomaly_check = next(
        c for c in report.checks if c.check_name == "statistical_mad_anomaly_check"
    )
    assert anomaly_check.dimension == "statistical_distribution"
    assert anomaly_check.violations >= 1  # row_count 1 is an extreme drop
    assert anomaly_check.status == "failed"
