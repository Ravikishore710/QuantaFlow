"""Distributed partition locking using PostgreSQL advisory locks and atomic file locks."""

import contextlib
import json
import os
import zlib
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from src.common.db import DatabaseConnectionManager
from src.common.errors import PartitionLockAcquisitionError
from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


class PartitionLockManager:
    """Acquires exclusive partition locks across workers to prevent concurrent corruption."""

    def __init__(
        self,
        period: str,
        engine_mode: str = "auto",
        settings: Settings | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.period = period
        self.engine_mode = engine_mode.lower()
        self.settings = settings or get_settings()
        self.timeout_seconds = timeout_seconds

        self._acquired = False
        self._backend: str | None = None
        self._pg_conn: Any = None
        self._pg_key: int | None = None
        self._lock_file: Path | None = None

    def acquire(self) -> bool:
        """Acquires lock via PostgreSQL advisory lock or atomic file lock."""
        if self._acquired:
            return True

        if self.engine_mode != "duckdb":
            db_mgr = DatabaseConnectionManager(settings=self.settings, engine_mode=self.engine_mode)
            pg_conn = db_mgr.get_postgres_connection()
            if pg_conn is not None:
                key = zlib.crc32(f"quantaflow:partition:{self.period}".encode())
                try:
                    with pg_conn.cursor() as cur:
                        cur.execute("SELECT pg_try_advisory_lock(%s);", (key,))
                        row = cur.fetchone()
                        locked = bool(row[0]) if row else False
                        if locked:
                            self._acquired = True
                            self._backend = "postgres"
                            self._pg_conn = pg_conn
                            self._pg_key = key
                            logger.info("partition_lock_acquired_pg", period=self.period, key=key)
                            return True
                    pg_conn.close()
                    raise PartitionLockAcquisitionError(
                        self.period, "PostgreSQL advisory lock held by another session"
                    )
                except Exception as exc:
                    with contextlib.suppress(Exception):
                        pg_conn.close()
                    if isinstance(exc, PartitionLockAcquisitionError):
                        raise
                    logger.warning("pg_advisory_lock_failed_falling_back_to_file", error=str(exc))

        # Fallback to atomic file lock
        lock_dir = self.settings.manifest_dir / "locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        sanitized = self.period.replace("/", "_").replace("\\", "_")
        lock_file = lock_dir / f"{sanitized}.lock"

        if lock_file.exists():
            self._handle_existing_lock_file(lock_file)

        try:
            fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError as err:
            raise PartitionLockAcquisitionError(
                self.period, f"Lock file {lock_file.name} is actively held by another process"
            ) from err

        payload = json.dumps(
            {
                "pid": os.getpid(),
                "period": self.period,
                "acquired_at": datetime.now(UTC).isoformat(),
                "timeout_seconds": self.timeout_seconds,
            }
        )
        os.write(fd, payload.encode("utf-8"))
        os.close(fd)

        self._acquired = True
        self._backend = "file"
        self._lock_file = lock_file
        logger.info("partition_lock_acquired_file", period=self.period, lock_file=str(lock_file))
        return True

    def _handle_existing_lock_file(self, lock_file: Path) -> None:
        """Inspects existing lock file for stale TTL or dead process."""
        try:
            data = json.loads(lock_file.read_text(encoding="utf-8"))
            pid = data.get("pid")
            acquired_at_str = data.get("acquired_at")
            if acquired_at_str:
                acquired_time = datetime.fromisoformat(acquired_at_str)
                age_seconds = (datetime.now(UTC) - acquired_time).total_seconds()
                if age_seconds > self.timeout_seconds:
                    logger.warning(
                        "breaking_stale_lock_ttl_expired",
                        period=self.period,
                        age_seconds=round(age_seconds, 2),
                    )
                    lock_file.unlink(missing_ok=True)
                    return

            if pid and isinstance(pid, int):
                try:
                    import psutil

                    if not psutil.pid_exists(pid):
                        logger.warning("breaking_stale_lock_dead_pid", period=self.period, pid=pid)
                        lock_file.unlink(missing_ok=True)
                        return
                except (ImportError, Exception):
                    pass
        except Exception as exc:
            logger.warning("corrupt_lock_file_removing", error=str(exc))
            lock_file.unlink(missing_ok=True)

    def release(self) -> None:
        """Releases the held partition lock safely."""
        if not self._acquired:
            return

        if self._backend == "postgres" and self._pg_conn and self._pg_key is not None:
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (self._pg_key,))
                self._pg_conn.close()
            except Exception as exc:
                logger.warning("pg_advisory_unlock_error", error=str(exc))
            finally:
                self._pg_conn = None
                self._pg_key = None

        elif self._backend == "file" and self._lock_file:
            try:
                self._lock_file.unlink(missing_ok=True)
            except Exception as exc:
                logger.warning("file_lock_unlink_error", error=str(exc))
            finally:
                self._lock_file = None

        self._acquired = False
        logger.info("partition_lock_released", period=self.period)

    @property
    def is_locked(self) -> bool:
        return self._acquired

    def __enter__(self) -> "PartitionLockManager":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.release()
