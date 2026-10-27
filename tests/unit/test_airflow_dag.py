"""Unit tests verifying Airflow DAG structure, modular task execution, and SLA callbacks."""

import ast
import py_compile
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.config.settings import Settings
from src.orchestration.callbacks import on_failure_alert_callback, sla_miss_callback
from src.orchestration.tasks import (
    build_gold_marts_task,
    emit_lineage_task,
    evaluate_wap_quality_gate_task,
    ingest_bronze_task,
    load_warehouse_task,
    transform_silver_task,
    validate_ingress_task,
)

DAG_PATH = Path("airflow/dags/quanta_flow_monthly.py")


def test_airflow_dag_syntax_compiles() -> None:
    """Verify bytecode compilation of the DAG with zero syntax errors."""
    assert DAG_PATH.exists(), "quanta_flow_monthly.py must exist"
    compiled_path = py_compile.compile(str(DAG_PATH), doraise=True)
    assert compiled_path is not None


def test_airflow_dag_ast_structure() -> None:
    """Verify DAG AST contains modular stage tasks, SLA callbacks, and pipeline tags."""
    tree = ast.parse(DAG_PATH.read_text(encoding="utf-8"))
    func_names = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]

    assert "quanta_flow_monthly_pipeline" in func_names
    assert "stage_1_validate_ingress" in func_names
    assert "stage_2_ingest_bronze" in func_names
    assert "stage_3_transform_silver" in func_names
    assert "stage_4_evaluate_wap_gate" in func_names
    assert "stage_5_build_gold_marts" in func_names
    assert "stage_6_load_warehouse" in func_names
    assert "stage_7_emit_lineage" in func_names

    content = DAG_PATH.read_text(encoding="utf-8")
    assert "sla_miss_callback=sla_miss_callback" in content
    assert "on_failure_callback" in content
    assert "t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7" in content


def test_airflow_modular_tasks_execution(tmp_path: Path) -> None:
    """Verify each DAG stage task callable executes sequentially against offline fixtures."""
    period = "2024-01"
    settings = Settings(
        sample_dir=tmp_path / "samples",
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        quarantine_dir=tmp_path / "quarantine",
        reports_dir=tmp_path / "reports",
        manifest_dir=tmp_path / "manifests",
    )

    # 1. Ingress
    res_ingress = validate_ingress_task(period=period, skip_upload=True)
    assert res_ingress["status"] == "validated"

    # 2. Ingest Bronze
    res_bronze = ingest_bronze_task(period=period, settings=settings)
    assert res_bronze["status"] == "ingested"
    assert res_bronze["bronze_taxi_rows"] > 0

    # 3. Transform Silver
    res_silver = transform_silver_task(period=period, settings=settings)
    assert res_silver["status"] == "transformed"
    assert res_silver["clean_rows"] > 0

    # 4. Evaluate WAP Gate
    res_wap = evaluate_wap_quality_gate_task(period=period, settings=settings)
    assert res_wap["status"] == "passed"
    assert res_wap["overall_status"] == "passed"

    # 5. Build Gold Marts
    res_gold = build_gold_marts_task(period=period, settings=settings)
    assert res_gold["status"] == "marts_built"

    # 6. Load Warehouse (DuckDB isolated)
    res_load = load_warehouse_task(period=period, settings=settings)
    assert res_load["status"] == "loaded"
    assert res_load["hourly_loaded"] > 0

    # 7. Emit Lineage
    res_lineage = emit_lineage_task(period=period, settings=settings)
    assert res_lineage["status"] == "lineage_emitted"
    assert Path(res_lineage["artifact"]).is_file()


def test_on_failure_alert_callback() -> None:
    """Verify on_failure_alert_callback extracts task context and calls send_quality_alert."""
    mock_ti = MagicMock()
    mock_ti.task_id = "stage_3_transform_silver"
    mock_ti.dag_id = "quanta_flow_monthly_pipeline"

    context = {
        "task_instance": mock_ti,
        "logical_date": "2024-01-01T00:00:00Z",
        "exception": ValueError("Corrupt parquet block"),
    }

    with patch("src.orchestration.callbacks.send_quality_alert", return_value=True) as mock_alert:
        result = on_failure_alert_callback(context)
        assert result is True
        mock_alert.assert_called_once()
        payload = mock_alert.call_args[0][0]
        assert payload["event"] == "airflow_task_failed"
        assert payload["task_id"] == "stage_3_transform_silver"
        assert payload["exception_type"] == "ValueError"


def test_sla_miss_callback() -> None:
    """Verify sla_miss_callback extracts DAG and SLA context and dispatches alert."""
    mock_dag = MagicMock()
    mock_dag.dag_id = "quanta_flow_monthly_pipeline"
    mock_task = MagicMock()
    mock_task.task_id = "stage_6_load_warehouse"

    with patch("src.orchestration.callbacks.send_quality_alert", return_value=True) as mock_alert:
        result = sla_miss_callback(
            dag=mock_dag,
            task_list=[mock_task],
            blocking_task_list=[mock_task],
            slas=[MagicMock()],
            blocking_tis=[MagicMock()],
        )
        assert result is True
        mock_alert.assert_called_once()
        payload = mock_alert.call_args[0][0]
        assert payload["event"] == "airflow_sla_missed"
        assert "stage_6_load_warehouse" in payload["tasks_missed"]
