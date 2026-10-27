import json
import shutil
from collections.abc import Callable
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


class HealthApp:
    """Lightweight zero-dependency router exposing decorated endpoints."""

    def __init__(self) -> None:
        self.routes: dict[str, Callable[[], Any]] = {}

    def get(self, path: str) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        """Decorator to register a GET route handler."""

        def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
            self.routes[path] = func
            return func

        return decorator

    def route(self, path: str) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
        """Alternative decorator syntax alias."""
        return self.get(path)

    def dispatch(self, path: str) -> tuple[int, str, bytes] | None:
        """Dispatch an incoming path to its registered handler."""
        handler = self.routes.get(path)
        if not handler:
            return None

        result = handler()
        if isinstance(result, str):
            return 200, "text/plain; version=0.0.4", result.encode("utf-8")

        status_code = 200 if result.get("status") == "healthy" else 503
        payload = json.dumps(result, indent=2).encode("utf-8")
        return status_code, "application/json", payload


# Global router instance for decorator registration
app = HealthApp()
router = app


@app.get("/health")
def health_endpoint() -> dict[str, Any]:
    """Inspect general system operational health."""
    return check_system_health(engine_mode="auto")


@app.get("/healthz")
def liveness_endpoint() -> dict[str, Any]:
    """Kubernetes liveness probe endpoint."""
    return check_system_health(engine_mode="auto")


@app.get("/readyz")
def readiness_endpoint() -> dict[str, Any]:
    """Kubernetes readiness probe verifying database and storage connectivity."""
    return check_system_health(engine_mode="auto")


@app.get("/metrics")
def prometheus_metrics_endpoint() -> str:
    """Prometheus exposition metrics format for operational monitoring."""
    health = check_system_health(engine_mode="auto")
    is_healthy = 1 if health["status"] == "healthy" else 0
    free_gb = health["checks"].get("disk", {}).get("free_gb", 0.0)

    lines = [
        "# HELP quantaflow_health_status System operational status (1=healthy, 0=unhealthy)",
        "# TYPE quantaflow_health_status gauge",
        f"quantaflow_health_status {is_healthy}",
        "# HELP quantaflow_disk_free_gigabytes Free disk storage space in gigabytes",
        "# TYPE quantaflow_disk_free_gigabytes gauge",
        f"quantaflow_disk_free_gigabytes {free_gb}",
    ]
    return "\n".join(lines) + "\n"


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler dispatching requests through the decorated HealthApp router."""

    def do_GET(self) -> None:
        dispatched = app.dispatch(self.path)
        if dispatched is not None:
            status_code, content_type, payload = dispatched
            self.send_response(status_code)
            self.send_header("Content-Type", content_type)
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
    except (KeyboardInterrupt, SystemExit):
        logger.info("health_server_stopped")
    finally:
        server.server_close()
