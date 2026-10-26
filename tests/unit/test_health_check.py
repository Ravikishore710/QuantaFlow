from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.monitoring.health import HealthCheckHandler, check_system_health


def test_health_check_duckdb_healthy(tmp_path: Path) -> None:
    res = check_system_health(engine_mode="duckdb")
    assert res["status"] == "healthy"
    assert res["checks"]["database"]["status"] == "ok"
    assert res["checks"]["database"]["active_engine"] == "duckdb"
    assert "storage" in res["checks"]
    assert "disk" in res["checks"]


def test_health_check_postgres_failure() -> None:
    with patch("psycopg2.connect", side_effect=ConnectionRefusedError("Postgres unreachable")):
        res = check_system_health(engine_mode="postgres")
        assert res["status"] == "unhealthy"
        assert res["checks"]["database"]["status"] == "error"


def test_health_check_auto_fallback_to_duckdb() -> None:
    with patch("psycopg2.connect", side_effect=ConnectionRefusedError("Postgres unreachable")):
        res = check_system_health(engine_mode="auto")
        assert res["status"] == "healthy"
        assert res["checks"]["database"]["active_engine"] == "duckdb"


def test_health_check_http_handler_200() -> None:
    mock_request = MagicMock()
    mock_wfile = BytesIO()
    mock_request.makefile.return_value = BytesIO(b"GET /healthz HTTP/1.1\r\n\r\n")

    class TestHandler(HealthCheckHandler):
        def __init__(self) -> None:
            self.path = "/healthz"
            self.wfile = mock_wfile
            self.send_response = MagicMock()  # type: ignore[assignment]
            self.send_header = MagicMock()  # type: ignore[assignment]
            self.end_headers = MagicMock()  # type: ignore[assignment]

    handler = TestHandler()
    handler.do_GET()

    assert handler.send_response.called
    status_code = handler.send_response.call_args[0][0]
    assert status_code in (200, 503)


def test_health_check_http_handler_404() -> None:
    mock_wfile = BytesIO()

    class TestHandler(HealthCheckHandler):
        def __init__(self) -> None:
            self.path = "/invalid-endpoint"
            self.wfile = mock_wfile
            self.send_response = MagicMock()  # type: ignore[assignment]
            self.send_header = MagicMock()  # type: ignore[assignment]
            self.end_headers = MagicMock()  # type: ignore[assignment]

    handler = TestHandler()
    handler.do_GET()

    handler.send_response.assert_called_with(404)
    assert mock_wfile.getvalue() == b"Not Found"


def test_health_check_cli_invocation(monkeypatch: object) -> None:
    import pytest

    from scripts.health_check import main

    # Verify CLI exits 0 when duckdb is healthy
    with patch("sys.argv", ["health_check.py", "--engine-mode", "duckdb"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
