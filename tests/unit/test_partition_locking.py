"""Unit tests verifying distributed PartitionLockManager concurrency and stale recovery."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.common.errors import PartitionLockAcquisitionError
from src.common.locking import PartitionLockManager
from src.config.settings import Settings


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    return Settings(
        manifest_dir=tmp_path / "manifests",
        engine_mode="duckdb",
    )


def test_file_lock_lifecycle_and_context_manager(test_settings: Settings) -> None:
    """Verify clean file-based lock acquire, existence check, and automatic release."""
    period = "2024-01"
    lock_mgr = PartitionLockManager(period=period, engine_mode="duckdb", settings=test_settings)

    assert not lock_mgr.is_locked
    with lock_mgr:
        assert lock_mgr.is_locked
        lock_file = test_settings.manifest_dir / "locks" / f"{period}.lock"
        assert lock_file.is_file()
        metadata = json.loads(lock_file.read_text(encoding="utf-8"))
        assert metadata["period"] == period
        assert "pid" in metadata
        assert "acquired_at" in metadata

    assert not lock_mgr.is_locked
    assert not lock_file.exists()


def test_concurrent_partition_lock_rejection(test_settings: Settings) -> None:
    """Verify that acquiring an already held partition lock raises PartitionLockAcquisitionError."""
    period = "2024-02"
    lock1 = PartitionLockManager(period=period, engine_mode="duckdb", settings=test_settings)
    lock2 = PartitionLockManager(period=period, engine_mode="duckdb", settings=test_settings)

    assert lock1.acquire() is True
    assert lock1.is_locked

    with pytest.raises(PartitionLockAcquisitionError) as exc_info:
        lock2.acquire()

    assert period in str(exc_info.value)
    assert not lock2.is_locked

    lock1.release()
    assert not lock1.is_locked

    # lock2 can now acquire
    assert lock2.acquire() is True
    assert lock2.is_locked
    lock2.release()


def test_stale_lock_ttl_cleanup(test_settings: Settings) -> None:
    """Verify that an expired lock file past timeout_seconds is broken and re-acquired."""
    period = "2024-03"
    lock_dir = test_settings.manifest_dir / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    stale_file = lock_dir / f"{period}.lock"

    stale_time = (datetime.now(UTC) - timedelta(seconds=120)).isoformat()
    stale_file.write_text(
        json.dumps(
            {
                "pid": 999999,
                "period": period,
                "acquired_at": stale_time,
                "timeout_seconds": 30.0,
            }
        ),
        encoding="utf-8",
    )

    mgr = PartitionLockManager(
        period=period,
        engine_mode="duckdb",
        settings=test_settings,
        timeout_seconds=30.0,
    )
    assert mgr.acquire() is True
    assert mgr.is_locked
    mgr.release()
    assert not stale_file.exists()


def test_idempotent_double_release(test_settings: Settings) -> None:
    """Verify multiple calls to release do not raise errors."""
    mgr = PartitionLockManager(period="2024-04", engine_mode="duckdb", settings=test_settings)
    mgr.acquire()
    mgr.release()
    # Second release should be a safe no-op
    mgr.release()
    assert not mgr.is_locked


def test_postgres_advisory_lock_success(test_settings: Settings) -> None:
    """Verify PostgreSQL advisory lock acquisition and unlock execution."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (True,)

    with patch(
        "src.common.locking.DatabaseConnectionManager.get_postgres_connection",
        return_value=mock_conn,
    ):
        mgr = PartitionLockManager(period="2024-05", engine_mode="postgres", settings=test_settings)
        assert mgr.acquire() is True
        assert mgr.is_locked
        assert mock_cursor.execute.call_count == 1
        sql, params = mock_cursor.execute.call_args[0]
        assert "pg_try_advisory_lock" in sql
        assert isinstance(params[0], int)

        mgr.release()
        assert not mgr.is_locked
        assert mock_cursor.execute.call_count == 2
        sql_u, params_u = mock_cursor.execute.call_args[0]
        assert "pg_advisory_unlock" in sql_u
        assert params_u == params


def test_postgres_advisory_lock_rejection_raises(test_settings: Settings) -> None:
    """Verify that PostgreSQL advisory lock contention raises PartitionLockAcquisitionError."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (False,)

    with patch(
        "src.common.locking.DatabaseConnectionManager.get_postgres_connection",
        return_value=mock_conn,
    ):
        mgr = PartitionLockManager(period="2024-06", engine_mode="postgres", settings=test_settings)
        with pytest.raises(PartitionLockAcquisitionError) as exc_info:
            mgr.acquire()
        assert "2024-06" in str(exc_info.value)
        assert not mgr.is_locked
