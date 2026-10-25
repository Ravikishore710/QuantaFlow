from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from src.common.db import DatabaseConnectionManager
from src.common.errors import WarehouseConnectionError
from src.config.settings import Settings


def test_db_manager_duckdb_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    manager = DatabaseConnectionManager(engine_mode="duckdb", duckdb_path=db_path)

    # Postgres connection is None in duckdb mode
    assert manager.get_postgres_connection() is None

    # DuckDB connects and runs query
    with manager.duckdb_connection() as con:
        con.execute("CREATE TABLE test_tab (id INT, name VARCHAR);")
        con.execute("INSERT INTO test_tab VALUES (1, 'quanta');")
        res = con.execute("SELECT name FROM test_tab WHERE id = 1;").fetchone()
        assert res is not None
        assert res[0] == "quanta"

    assert db_path.exists()


def test_db_manager_postgres_strict_mode_raises(tmp_path: Path) -> None:
    settings = Settings(
        postgres_host="unreachable.example.com",
        postgres_port=5432,
    )
    manager = DatabaseConnectionManager(
        settings=settings,
        engine_mode="postgres",
        duckdb_path=tmp_path / "test.duckdb",
    )

    with (
        patch("psycopg2.connect", side_effect=psycopg2.OperationalError("Connection refused")),
        pytest.raises(WarehouseConnectionError, match="PostgreSQL production warehouse"),
    ):
        manager.get_postgres_connection()


def test_db_manager_auto_mode_fallback_to_duckdb(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback.duckdb"
    settings = Settings(
        postgres_host="unreachable.example.com",
        postgres_port=5432,
    )
    manager = DatabaseConnectionManager(
        settings=settings,
        engine_mode="auto",
        duckdb_path=db_path,
    )

    with patch("psycopg2.connect", side_effect=ConnectionRefusedError("Connection refused")):
        # In auto mode, postgres connection gracefully returns None and logs warning
        conn = manager.get_postgres_connection()
        assert conn is None

        # DuckDB fallback is fully usable
        with manager.duckdb_connection() as con:
            con.execute("CREATE TABLE auto_test (val INT);")
            con.execute("INSERT INTO auto_test VALUES (42);")
            val = con.execute("SELECT val FROM auto_test;").fetchone()
            assert val is not None
            assert val[0] == 42


def test_db_manager_postgres_connection_context_manager() -> None:
    mock_conn = MagicMock()
    manager = DatabaseConnectionManager(engine_mode="postgres")

    with patch.object(manager, "get_postgres_connection", return_value=mock_conn):
        with manager.postgres_connection() as conn:
            assert conn == mock_conn
        assert mock_conn.close.called


def test_db_manager_duckdb_connect_failure_raises(tmp_path: Path) -> None:
    manager = DatabaseConnectionManager(
        engine_mode="duckdb",
        duckdb_path=tmp_path / "fail.duckdb",
    )
    with (
        patch("duckdb.connect", side_effect=RuntimeError("DuckDB lock collision")),
        pytest.raises(WarehouseConnectionError, match="DuckDB warehouse connection failed"),
    ):
        manager.get_duckdb_connection()
