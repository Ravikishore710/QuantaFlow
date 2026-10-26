import logging
import sys
from collections.abc import MutableMapping
from contextvars import ContextVar
from typing import Any

import structlog

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)
run_id_var: ContextVar[str | None] = ContextVar("run_id", default=None)


def set_correlation_context(correlation_id: str | None = None, run_id: str | None = None) -> None:
    """Set active correlation ID and run ID in the thread/async context."""
    if correlation_id:
        correlation_id_var.set(correlation_id)
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
    if run_id:
        run_id_var.set(run_id)
        structlog.contextvars.bind_contextvars(run_id=run_id)


def add_correlation_ids(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Inject active correlation_id and run_id into structlog event dictionary."""
    cid = correlation_id_var.get()
    if cid and "correlation_id" not in event_dict:
        event_dict["correlation_id"] = cid

    rid = run_id_var.get()
    if rid and "run_id" not in event_dict:
        event_dict["run_id"] = rid

    return event_dict


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            add_correlation_ids,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
