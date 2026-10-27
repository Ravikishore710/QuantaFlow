"""Airflow failure and SLA alert callbacks for pipeline observability."""

from typing import Any

from src.common.logging import get_logger
from src.config.settings import get_settings
from src.validation.quality_framework import send_quality_alert

logger = get_logger(__name__)


def on_failure_alert_callback(context: dict[str, Any]) -> bool:
    """Airflow on_failure_callback dispatching error alert to configured webhook."""
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", str(task_instance or "unknown_task"))
    dag_id = getattr(task_instance, "dag_id", "quanta_flow_monthly_pipeline")
    exception = context.get("exception")

    alert_payload = {
        "event": "airflow_task_failed",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": str(context.get("logical_date") or context.get("execution_date", "")),
        "exception_type": type(exception).__name__ if exception else "UnknownException",
        "exception_message": (
            str(exception) if exception else "Task failed without exception object"
        ),
    }
    logger.error(
        "airflow_task_failure_alert_triggered",
        dag_id=dag_id,
        task_id=task_id,
        exception_type=alert_payload["exception_type"],
        exception_message=alert_payload["exception_message"],
    )

    settings = get_settings()
    return send_quality_alert(alert_payload, webhook_url=settings.alert_webhook_url)


def sla_miss_callback(
    dag: Any,
    task_list: Any,
    blocking_task_list: Any,
    slas: Any,
    blocking_tis: Any,
) -> bool:
    """Airflow sla_miss_callback alerting operations team of delayed pipeline execution."""
    dag_id = getattr(dag, "dag_id", str(dag or "quanta_flow_monthly_pipeline"))
    task_ids = [getattr(t, "task_id", str(t)) for t in (task_list or [])]

    alert_payload = {
        "event": "airflow_sla_missed",
        "dag_id": dag_id,
        "tasks_missed": task_ids,
        "slas_count": len(slas) if slas else 0,
        "blocking_tasks": [getattr(t, "task_id", str(t)) for t in (blocking_task_list or [])],
    }
    logger.warning(
        "airflow_sla_miss_alert_triggered",
        dag_id=dag_id,
        tasks_missed=task_ids,
        slas_count=alert_payload["slas_count"],
    )

    settings = get_settings()
    return send_quality_alert(alert_payload, webhook_url=settings.alert_webhook_url)
