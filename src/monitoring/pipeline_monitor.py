import json
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.common.db import PG_ERRORS, DatabaseConnectionManager
from src.common.logging import get_logger
from src.config.settings import Settings, get_settings
from src.validation.quality_framework import QualitySuiteReport

logger = get_logger(__name__)


class PipelineMonitor:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._local_db_path = Path("data/quantaflow.duckdb")
        self.db_manager = DatabaseConnectionManager(
            settings=self.settings,
            engine_mode="auto",
            duckdb_path=self._local_db_path,
        )

    @property
    def local_db_path(self) -> Path:
        return self.db_manager.duckdb_path

    @local_db_path.setter
    def local_db_path(self, path: Path) -> None:
        self._local_db_path = path
        self.db_manager.duckdb_path = path

    def _get_postgres_conn(self) -> Any:
        return self.db_manager.get_postgres_connection()

    def _get_duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        return self.db_manager.get_duckdb_connection()

    def start_pipeline_run(
        self,
        run_id: str,
        pipeline_name: str,
        period: str,
    ) -> None:
        started_at = datetime.now(UTC)
        year, month = period.split("-")
        interval_start = date(int(year), int(month), 1)
        # last day of month
        if int(month) == 12:
            interval_end = date(int(year), 12, 31)
        else:
            interval_end = date(int(year), int(month) + 1, 1)

        # Postgres write
        conn = self._get_postgres_conn()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO monitoring.pipeline_runs (
                            run_id, pipeline_name, data_interval_start, data_interval_end,
                            started_at, status, code_version
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id) DO NOTHING;
                        """,
                        (
                            uuid.UUID(hex=run_id),
                            pipeline_name,
                            interval_start,
                            interval_end,
                            started_at,
                            "running",
                            self.settings.pipeline_version,
                        ),
                    )
                conn.commit()
            except PG_ERRORS as exc:
                logger.debug("postgres_pipeline_run_start_fallback", error=str(exc))
            finally:
                conn.close()

        # Local DuckDB write
        try:
            con = self._get_duckdb_conn()
            con.execute("CREATE SCHEMA IF NOT EXISTS monitoring;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
                    run_id VARCHAR PRIMARY KEY,
                    pipeline_name VARCHAR,
                    data_interval_start DATE,
                    data_interval_end DATE,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    status VARCHAR,
                    rows_in BIGINT,
                    rows_out BIGINT,
                    rows_rejected BIGINT,
                    code_version VARCHAR
                );
                """
            )
            con.execute(
                """
                INSERT INTO monitoring.pipeline_runs (
                    run_id, pipeline_name, data_interval_start, data_interval_end,
                    started_at, status, code_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id) DO NOTHING;
                """,
                [
                    run_id,
                    pipeline_name,
                    interval_start,
                    interval_end,
                    started_at,
                    "running",
                    self.settings.pipeline_version,
                ],
            )
            con.close()
        except (duckdb.Error, OSError) as exc:
            logger.debug("duckdb_pipeline_run_start_fallback", error=str(exc))

    def finish_pipeline_run(
        self,
        run_id: str,
        status: str,
        rows_in: int,
        rows_out: int,
        rows_rejected: int,
    ) -> None:
        finished_at = datetime.now(UTC)

        # Postgres update
        conn = self._get_postgres_conn()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE monitoring.pipeline_runs
                        SET finished_at = %s, status = %s, rows_in = %s,
                            rows_out = %s, rows_rejected = %s
                        WHERE run_id = %s;
                        """,
                        (
                            finished_at,
                            status,
                            rows_in,
                            rows_out,
                            rows_rejected,
                            uuid.UUID(hex=run_id),
                        ),
                    )
                conn.commit()
            except PG_ERRORS as exc:
                logger.debug("postgres_pipeline_run_finish_fallback", error=str(exc))
            finally:
                conn.close()

        # Local DuckDB update
        try:
            con = self._get_duckdb_conn()
            con.execute(
                """
                UPDATE monitoring.pipeline_runs
                SET finished_at = ?, status = ?, rows_in = ?, rows_out = ?, rows_rejected = ?
                WHERE run_id = ?;
                """,
                [finished_at, status, rows_in, rows_out, rows_rejected, run_id],
            )
            con.close()
        except (duckdb.Error, OSError) as exc:
            logger.debug("duckdb_pipeline_run_finish_fallback", error=str(exc))

    def record_quality_results(self, report: QualitySuiteReport) -> None:
        conn = self._get_postgres_conn()
        if conn:
            try:
                with conn.cursor() as cur:
                    for check in report.checks:
                        cur.execute(
                            """
                            INSERT INTO monitoring.quality_results (
                                run_id, dataset, partition, check_name,
                                severity, rows_checked, violations, status, details
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (
                                uuid.UUID(hex=report.run_id),
                                report.dataset,
                                report.partition,
                                check.check_name,
                                check.severity,
                                check.rows_checked,
                                check.violations,
                                check.status,
                                json.dumps(check.details),
                            ),
                        )
                conn.commit()
            except PG_ERRORS as exc:
                logger.debug("postgres_quality_results_fallback", error=str(exc))
            finally:
                conn.close()

        try:
            con = self._get_duckdb_conn()
            con.execute("CREATE SCHEMA IF NOT EXISTS monitoring;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS monitoring.quality_results (
                    run_id VARCHAR,
                    dataset VARCHAR,
                    partition VARCHAR,
                    check_name VARCHAR,
                    severity VARCHAR,
                    rows_checked BIGINT,
                    violations BIGINT,
                    status VARCHAR,
                    details VARCHAR
                );
                """
            )
            for check in report.checks:
                con.execute(
                    """
                    INSERT INTO monitoring.quality_results (
                        run_id, dataset, partition, check_name,
                        severity, rows_checked, violations, status, details
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    [
                        report.run_id,
                        report.dataset,
                        report.partition,
                        check.check_name,
                        check.severity,
                        check.rows_checked,
                        check.violations,
                        check.status,
                        json.dumps(check.details),
                    ],
                )
            con.close()
        except (duckdb.Error, OSError) as exc:
            logger.debug("duckdb_quality_results_fallback", error=str(exc))
