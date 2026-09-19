from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

DEFAULT_PERIOD = "2024-01"


@dag(
    dag_id="quanta_flow_monthly_pipeline",
    description="Incremental multi-source ingestion, bronze/silver/gold transformation, quality validation, and warehouse loading",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"period": Param(DEFAULT_PERIOD, type="string", pattern=r"^\d{4}-\d{2}$")},
    tags=["quanta-flow", "etl", "production"],
)
def quanta_flow_monthly_pipeline() -> None:
    @task
    def run_end_to_end_pipeline(period: str) -> dict:
        """Executes the full pipeline: ingestion, bronze, silver, gold, quality, and warehouse load."""
        from scripts.run_pipeline import run_pipeline

        exit_code = run_pipeline(period=period, skip_upload=False)
        if exit_code != 0:
            raise RuntimeError(f"QuantaFlow pipeline execution failed for period {period}")
        return {"period": period, "status": "success"}

    period = "{{ params.period }}"
    run_end_to_end_pipeline(period)


quanta_flow_monthly_pipeline()
