"""Centralized database connection manager for PostgreSQL and DuckDB."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from src.common.errors import WarehouseConnectionError
from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

try:
    import psycopg2

    PG_ERRORS: tuple[type[Exception], ...] = (
        psycopg2.OperationalError,
        psycopg2.Error,
        OSError,
        ConnectionError,
    )
except ImportError:  # pragma: no cover
    PG_ERRORS = (OSError, ConnectionError)

logger = get_logger(__name__)


class DatabaseConnectionManager:
    """Manages connections and transactions for PostgreSQL and DuckDB."""

    def __init__(
        self,
        settings: Settings | None = None,
        engine_mode: str = "auto",
        duckdb_path: Path | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.engine_mode = engine_mode.lower()
        self.duckdb_path = duckdb_path or Path("data/quantaflow.duckdb")

    def get_postgres_connection(self) -> Any:
        """Acquires a PostgreSQL connection or handles fallback based on engine_mode."""
        if self.engine_mode == "duckdb":
            return None

        try:
            try:
                import psycopg

                return psycopg.connect(
                    host=self.settings.postgres_host,
                    port=self.settings.postgres_port,
                    dbname=self.settings.postgres_db,
                    user=self.settings.postgres_user,
                    password=self.settings.postgres_password,
                    connect_timeout=2,
                )
            except ImportError:
                import psycopg2

                return psycopg2.connect(
                    host=self.settings.postgres_host,
                    port=self.settings.postgres_port,
                    dbname=self.settings.postgres_db,
                    user=self.settings.postgres_user,
                    password=self.settings.postgres_password,
                    connect_timeout=2,
                )
        except (psycopg2.Error, OSError, ConnectionError, Exception) as exc:
            if self.engine_mode == "postgres":
                raise WarehouseConnectionError(
                    f"PostgreSQL production warehouse connection failed: {exc}. "
                    f"Target host {self.settings.postgres_host}:{self.settings.postgres_port}."
                ) from exc

            logger.warning(
                "postgres_unavailable_falling_back_to_duckdb",
                host=self.settings.postgres_host,
                port=self.settings.postgres_port,
                error=str(exc),
            )
            return None

    def get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Acquires a DuckDB connection, ensuring directory existence."""
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return duckdb.connect(str(self.duckdb_path))
        except (duckdb.Error, OSError, RuntimeError) as exc:
            raise WarehouseConnectionError(
                f"DuckDB warehouse connection failed at {self.duckdb_path}: {exc}"
            ) from exc

    @contextmanager
    def postgres_connection(self) -> Generator[Any, None, None]:
        """Context manager for PostgreSQL connection."""
        conn = self.get_postgres_connection()
        try:
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def duckdb_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Context manager for DuckDB connection."""
        conn = self.get_duckdb_connection()
        try:
            yield conn
        finally:
            conn.close()
