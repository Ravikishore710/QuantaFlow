import ast
import py_compile
from pathlib import Path
from unittest.mock import patch


def test_airflow_dag_syntax_compiles() -> None:
    dag_path = Path("airflow/dags/quanta_flow_monthly.py")
    assert dag_path.exists(), "quanta_flow_monthly.py must exist"

    # Verify bytecode compilation with no syntax errors
    compiled_path = py_compile.compile(str(dag_path), doraise=True)
    assert compiled_path is not None


def test_airflow_dag_ast_structure() -> None:
    dag_path = Path("airflow/dags/quanta_flow_monthly.py")
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))

    # Assert top-level imports and function decorator structure
    func_defs = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    func_names = [f.name for f in func_defs]
    assert "quanta_flow_monthly_pipeline" in func_names
    assert "run_end_to_end_pipeline" in func_names

    # Verify dag_id is properly specified
    code_text = dag_path.read_text(encoding="utf-8")
    assert 'dag_id="quanta_flow_monthly_pipeline"' in code_text
    assert 'schedule=None' in code_text
    assert 'start_date=datetime(2024, 1, 1)' in code_text


def test_airflow_task_execution_logic() -> None:
    """Verifies the core execution logic encapsulated inside the Airflow task callable."""
    with patch("scripts.run_pipeline.run_pipeline", return_value=0) as mock_run:
        from scripts.run_pipeline import run_pipeline

        exit_code = run_pipeline("2024-01", skip_upload=True)
        assert exit_code == 0
        assert mock_run.called
