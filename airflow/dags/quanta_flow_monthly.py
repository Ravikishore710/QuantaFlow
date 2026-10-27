"""QuantaFlow monthly analytical data engineering pipeline DAG."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

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

DEFAULT_PERIOD = "2024-01"


@dag(
    dag_id="quanta_flow_monthly_pipeline",
    description=(
        "Incremental multi-source ingestion, bronze/silver/gold transformation, "
        "WAP quality gate, warehouse loading, and OpenLineage tracking"
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    default_args={
        "owner": "quantaflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": on_failure_alert_callback,
    },
    params={"period": Param(DEFAULT_PERIOD, type="string", pattern=r"^\d{4}-\d{2}$")},
    tags=["quanta-flow", "etl", "production", "medallion"],
)
def quanta_flow_monthly_pipeline() -> None:
    @task
    def stage_1_validate_ingress(period: str) -> dict:
        return validate_ingress_task(period=period, skip_upload=True)

    @task
    def stage_2_ingest_bronze(period: str) -> dict:
        return ingest_bronze_task(period=period)

    @task
    def stage_3_transform_silver(period: str) -> dict:
        return transform_silver_task(period=period)

    @task
    def stage_4_evaluate_wap_gate(period: str) -> dict:
        return evaluate_wap_quality_gate_task(period=period)

    @task
    def stage_5_build_gold_marts(period: str) -> dict:
        return build_gold_marts_task(period=period)

    @task
    def stage_6_load_warehouse(period: str) -> dict:
        return load_warehouse_task(period=period)

    @task
    def stage_7_emit_lineage(period: str) -> dict:
        return emit_lineage_task(period=period)

    period = "{{ params.period }}"
    t1 = stage_1_validate_ingress(period)
    t2 = stage_2_ingest_bronze(period)
    t3 = stage_3_transform_silver(period)
    t4 = stage_4_evaluate_wap_gate(period)
    t5 = stage_5_build_gold_marts(period)
    t6 = stage_6_load_warehouse(period)
    t7 = stage_7_emit_lineage(period)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7


quanta_flow_monthly_pipeline()
