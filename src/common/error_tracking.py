from typing import Any

import sentry_sdk
import structlog
from sentry_sdk.integrations.logging import LoggingIntegration

logger = structlog.get_logger(__name__)


def init_error_tracking(
    dsn: str | None,
    environment: str = "production",
    release: str = "0.1.0",
) -> bool:
    """Initialize Sentry error tracking with environment context and tags."""
    if not dsn or not dsn.strip():
        logger.debug("sentry_skipped_no_dsn")
        return False

    try:
        sentry_logging = LoggingIntegration(
            level=None,  # Do not capture standard logging records as breadcrumbs
            event_level=None,  # Send errors manually
        )
        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            release=release,
            traces_sample_rate=1.0,
            integrations=[sentry_logging],
        )
        logger.info("sentry_error_tracking_initialized", environment=environment, release=release)
        return True
    except Exception as exc:
        logger.warning("sentry_init_failed", error=str(exc))
        return False


def capture_pipeline_exception(
    exc: BaseException,
    context: dict[str, Any] | None = None,
) -> str | None:
    """Capture exception to Sentry with pipeline metadata tags and structured log."""
    ctx = context or {}
    logger.error("pipeline_exception_captured", error=str(exc), **ctx)

    try:
        with sentry_sdk.push_scope() as scope:
            for key, val in ctx.items():
                scope.set_tag(key, str(val))
            event_id = sentry_sdk.capture_exception(exc)
            return str(event_id) if event_id else None
    except Exception as err:
        logger.warning("sentry_capture_failed", error=str(err))
        return None
