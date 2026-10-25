import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from src.config.settings import Settings
from src.monitoring.pipeline_monitor import PipelineMonitor
from src.validation.quality_framework import QualityCheckResult, QualitySuiteReport


@pytest.fixture
def monitor(tmp_path: Path) -> PipelineMonitor:
    settings = Settings(
        postgres_host="invalid.host",
        postgres_port=9999,
        postgres_db="test",
        postgres_user="test",
        postgres_password="test",
        manifest_dir=tmp_path / "manifests",
    )
    mon = PipelineMonitor(settings=settings)
    mon.local_db_path = tmp_path / "monitor.duckdb"
    return mon


def test_pipeline_monitor_start_and_finish_run_duckdb(monitor: PipelineMonitor) -> None:
    run_id = uuid.uuid4().hex
    monitor.start_pipeline_run(run_id=run_id, pipeline_name="nyc_mobility", period="2024-01")

    con = duckdb.connect(str(monitor.local_db_path))
    query = "SELECT run_id, pipeline_name, status FROM monitoring.pipeline_runs WHERE run_id = ?"
    res = con.execute(query, [run_id]).fetchall()
    assert len(res) == 1
    assert res[0][1] == "nyc_mobility"
    assert res[0][2] == "running"

    monitor.finish_pipeline_run(
        run_id=run_id, status="success", rows_in=100, rows_out=95, rows_rejected=5
    )
    update_q = (
        "SELECT status, rows_in, rows_out, rows_rejected "
        "FROM monitoring.pipeline_runs WHERE run_id = ?"
    )
    res_after = con.execute(update_q, [run_id]).fetchall()
    con.close()

    assert len(res_after) == 1
    assert res_after[0][0] == "success"
    assert res_after[0][1] == 100
    assert res_after[0][2] == 95
    assert res_after[0][3] == 5


def test_pipeline_monitor_december_interval(monitor: PipelineMonitor) -> None:
    run_id = uuid.uuid4().hex
    monitor.start_pipeline_run(run_id=run_id, pipeline_name="nyc_mobility", period="2024-12")

    con = duckdb.connect(str(monitor.local_db_path))
    q = (
        "SELECT strftime(data_interval_end, '%Y-%m-%d') "
        "FROM monitoring.pipeline_runs WHERE run_id = ?"
    )
    res = con.execute(q, [run_id]).fetchone()
    con.close()
    assert res is not None
    assert res[0] == "2024-12-31"


def test_pipeline_monitor_record_quality_results(monitor: PipelineMonitor) -> None:
    run_id = uuid.uuid4().hex
    report = QualitySuiteReport(
        dataset="nyc_tlc_yellow",
        partition="2024-01",
        run_id=run_id,
        evaluated_at="2024-01-01T00:00:00",
        overall_status="passed",
        total_checks=1,
        passed_checks=1,
        failed_checks=0,
        checks=[
            QualityCheckResult(
                check_name="null_rate_pickup_datetime",
                dimension="completeness",
                severity="critical",
                status="passed",
                rows_checked=1000,
                violations=0,
                details={"threshold": 0.0, "actual": 0.0},
            )
        ],
    )

    monitor.record_quality_results(report)

    con = duckdb.connect(str(monitor.local_db_path))
    q = (
        "SELECT run_id, check_name, status, rows_checked "
        "FROM monitoring.quality_results WHERE run_id = ?"
    )
    res = con.execute(q, [run_id]).fetchall()
    con.close()

    assert len(res) == 1
    assert res[0][1] == "null_rate_pickup_datetime"
    assert res[0][2] == "passed"
    assert res[0][3] == 1000


def test_pipeline_monitor_postgres_flow(monitor: PipelineMonitor) -> None:
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch.object(monitor, "_get_postgres_conn", return_value=mock_conn):
        run_id = uuid.uuid4().hex
        monitor.start_pipeline_run(run_id, "nyc_mobility", "2024-01")
        assert mock_cur.execute.called
        assert mock_conn.commit.called

        mock_cur.reset_mock()
        mock_conn.reset_mock()
        monitor.finish_pipeline_run(run_id, "success", 10, 10, 0)
        assert mock_cur.execute.called
        assert mock_conn.commit.called
