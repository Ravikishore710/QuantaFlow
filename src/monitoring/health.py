import json
import shutil
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

from src.common.db import DatabaseConnectionManager
from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


def check_system_health(
    engine_mode: str = "auto", settings: Settings | None = None
) -> dict[str, Any]:
    """Inspect database connectivity, storage readiness, and system resources."""
    settings = settings or get_settings()
    now_iso = datetime.now(UTC).isoformat()
    checks: dict[str, dict[str, Any]] = {}
    is_healthy = True

    # 1. Database Connectivity Check
    db_manager = DatabaseConnectionManager(
        settings=settings,
        engine_mode=engine_mode,
        duckdb_path=Path("data/quantaflow.duckdb"),
    )
    db_check: dict[str, Any] = {"status": "ok", "engine_mode": engine_mode}

    if engine_mode == "postgres":
        try:
            conn = db_manager.get_postgres_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                conn.close()
                db_check["active_engine"] = "postgres"
            else:
                db_check["status"] = "error"
                db_check["error"] = "postgres_connection_failed"
                is_healthy = False
        except Exception as exc:
            db_check["status"] = "error"
            db_check["error"] = str(exc)
            is_healthy = False
    elif engine_mode == "duckdb":
        try:
            con = db_manager.get_duckdb_connection()
            res = con.execute("SELECT 1;").fetchone()
            con.close()
            db_check["active_engine"] = "duckdb"
            if not res or res[0] != 1:
                db_check["status"] = "error"
                is_healthy = False
        except Exception as exc:
            db_check["status"] = "error"
            db_check["error"] = str(exc)
            is_healthy = False
    else:  # "auto"
        try:
            conn = db_manager.get_postgres_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                conn.close()
                db_check["active_engine"] = "postgres"
            else:
                con = db_manager.get_duckdb_connection()
                con.execute("SELECT 1;")
                con.close()
                db_check["active_engine"] = "duckdb"
        except Exception as exc:
            try:
                con = db_manager.get_duckdb_connection()
                con.execute("SELECT 1;")
                con.close()
                db_check["active_engine"] = "duckdb"
            except Exception as duck_exc:
                db_check["status"] = "error"
                db_check["error"] = f"postgres_err: {exc}; duckdb_err: {duck_exc}"
                is_healthy = False

    checks["database"] = db_check

    # 2. Local Storage Readiness Check
    storage_check: dict[str, Any] = {"status": "ok"}
    try:
        sample_path = settings.bronze_dir
        sample_path.mkdir(parents=True, exist_ok=True)
        probe_file = sample_path / ".health_probe"
        probe_file.write_text("probe", encoding="utf-8")
        probe_file.unlink()
        storage_check["writable"] = True
    except Exception as exc:
        storage_check["status"] = "error"
        storage_check["error"] = str(exc)
        is_healthy = False
    checks["storage"] = storage_check

    # 3. Disk Space Check
    try:
        usage = shutil.disk_usage(Path("."))
        free_gb = round(usage.free / (1024**3), 2)
        checks["disk"] = {"status": "ok", "free_gb": free_gb}
        if free_gb < 0.1:  # Less than 100MB free
            checks["disk"]["status"] = "warning"
    except Exception as exc:
        checks["disk"] = {"status": "unknown", "error": str(exc)}

    status_str = "healthy" if is_healthy else "unhealthy"
    return {
        "status": status_str,
        "timestamp": now_iso,
        "checks": checks,
    }


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Lightweight HTTP request handler for container readiness and liveness probes."""

    def do_GET(self) -> None:
        if self.path in ("/health", "/healthz", "/readyz"):
            result = check_system_health(engine_mode="auto")
            status_code = 200 if result["status"] == "healthy" else 503
            payload = json.dumps(result, indent=2).encode("utf-8")

            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress noisy standard HTTP access logs
        pass


def run_health_server(host: str = "0.0.0.0", port: int = 8081) -> None:
    server = HTTPServer((host, port), HealthCheckHandler)
    logger.info("health_server_started", host=host, port=port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
