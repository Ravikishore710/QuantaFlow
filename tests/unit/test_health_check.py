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

    # Verify CLI exits 1 when unhealthy
    with (
        patch("scripts.health_check.check_system_health", return_value={"status": "unhealthy"}),
        patch("sys.argv", ["health_check.py", "--engine-mode", "duckdb"]),
        pytest.raises(SystemExit) as exc_info,
    ):
        main()
    assert exc_info.value.code == 1

    # Verify CLI runs health server
    with (
        patch("scripts.health_check.run_health_server") as mock_srv,
        patch("sys.argv", ["health_check.py", "--serve", "--port", "9999"]),
    ):
        main()
        mock_srv.assert_called_once_with(host="0.0.0.0", port=9999)


def test_health_check_auto_postgres_success() -> None:
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    with patch(
        "src.common.db.DatabaseConnectionManager.get_postgres_connection",
        return_value=mock_conn,
    ):
        res = check_system_health(engine_mode="auto")
        assert res["status"] == "healthy"
        assert res["checks"]["database"]["active_engine"] == "postgres"


def test_health_check_auto_both_fail() -> None:
    with (
        patch(
            "src.common.db.DatabaseConnectionManager.get_postgres_connection",
            side_effect=RuntimeError("pg dead"),
        ),
        patch(
            "src.common.db.DatabaseConnectionManager.get_duckdb_connection",
            side_effect=RuntimeError("duck dead"),
        ),
    ):
        res = check_system_health(engine_mode="auto")
        assert res["status"] == "unhealthy"
        assert res["checks"]["database"]["status"] == "error"


def test_health_check_storage_failure(tmp_path: Path) -> None:
    with patch("pathlib.Path.mkdir", side_effect=PermissionError("read only fs")):
        res = check_system_health(engine_mode="duckdb")
        assert res["status"] == "unhealthy"
        assert res["checks"]["storage"]["status"] == "error"


def test_health_check_disk_warning_and_error() -> None:
    # Warning when free < 100MB
    with patch("shutil.disk_usage", return_value=MagicMock(free=50 * 1024 * 1024)):
        res = check_system_health(engine_mode="duckdb")
        assert res["checks"]["disk"]["status"] == "warning"

    # Error when disk_usage raises
    with patch("shutil.disk_usage", side_effect=OSError("disk error")):
        res = check_system_health(engine_mode="duckdb")
        assert res["checks"]["disk"]["status"] == "unknown"


def test_health_server_run_and_handler_log() -> None:
    from src.monitoring.health import run_health_server

    # Verify log_message suppresses output without raising
    class DummyHandler(HealthCheckHandler):
        def __init__(self) -> None:
            pass

    dummy = DummyHandler()
    dummy.log_message("test %s", "arg")

    # Verify run_health_server starts and closes on interrupt
    with patch("src.monitoring.health.HTTPServer") as mock_server_cls:
        mock_instance = MagicMock()
        mock_instance.serve_forever.side_effect = KeyboardInterrupt()
        mock_server_cls.return_value = mock_instance
        run_health_server("127.0.0.1", 8089)
        mock_instance.server_close.assert_called_once()

