from unittest.mock import MagicMock, patch

from src.common.error_tracking import capture_pipeline_exception, init_error_tracking
from src.common.logging import add_correlation_ids, set_correlation_context
from src.common.run_context import RunContext


def test_init_error_tracking_without_dsn() -> None:
    assert init_error_tracking(None) is False
    assert init_error_tracking("") is False
    assert init_error_tracking("   ") is False


def test_init_error_tracking_with_dsn() -> None:
    with patch("sentry_sdk.init") as mock_init:
        success = init_error_tracking(
            dsn="https://key@o12345.ingest.sentry.io/67890",
            environment="staging",
            release="1.1.0",
        )
        assert success is True
        mock_init.assert_called_once()
        call_kwargs = mock_init.call_args[1]
        assert call_kwargs["dsn"] == "https://key@o12345.ingest.sentry.io/67890"
        assert call_kwargs["environment"] == "staging"
        assert call_kwargs["release"] == "1.1.0"


def test_capture_pipeline_exception_forwards_to_sentry() -> None:
    mock_scope = MagicMock()

    with (
        patch("sentry_sdk.push_scope") as mock_push_scope,
        patch("sentry_sdk.capture_exception", return_value="evt-12345") as mock_capture,
    ):
        mock_push_scope.return_value.__enter__.return_value = mock_scope

        err = ValueError("Invalid trip duration calculated")
        evt_id = capture_pipeline_exception(
            err,
            context={"run_id": "test_run_123", "layer": "silver", "period": "2024-01"},
        )

        assert evt_id == "evt-12345"
        mock_capture.assert_called_once_with(err)
        assert mock_scope.set_tag.call_count == 3
        mock_scope.set_tag.assert_any_call("run_id", "test_run_123")
        mock_scope.set_tag.assert_any_call("layer", "silver")
        mock_scope.set_tag.assert_any_call("period", "2024-01")


def test_correlation_id_and_run_id_injection_in_logs() -> None:
    run_ctx = RunContext()
    assert run_ctx.run_id is not None
    assert run_ctx.correlation_id is not None

    # Manually test the processor
    event_dict = {"event": "test_log_event"}
    enriched = add_correlation_ids(logger=None, method_name="info", event_dict=event_dict)

    assert enriched["run_id"] == run_ctx.run_id
    assert enriched["correlation_id"] == run_ctx.correlation_id


def test_set_correlation_context_updates_vars() -> None:
    set_correlation_context(correlation_id="corr-999", run_id="run-888")
    event_dict = {"event": "manual_context_event"}
    enriched = add_correlation_ids(logger=None, method_name="info", event_dict=event_dict)

    assert enriched["correlation_id"] == "corr-999"
    assert enriched["run_id"] == "run-888"
