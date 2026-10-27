from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from src.common.db import PG_ERRORS, DatabaseConnectionManager
from src.common.error_tracking import capture_pipeline_exception
from src.common.logging import get_logger
from src.config.settings import Settings, get_settings
from src.warehouse.postgres_staging import PostgresStagingBulkLoader

logger = get_logger(__name__)


class WarehouseLoader:
    def __init__(
        self,
        settings: Settings | None = None,
        engine_mode: str = "auto",
        duckdb_path: Path | None = None,
    ) -> None:
        """Initializes the warehouse loader.

        Parameters
        ----------
        engine_mode : str
            - 'postgres': Production target. Enforces PostgreSQL connection.
            - 'duckdb': Developer/offline mode. Targets local embedded DuckDB warehouse.
            - 'auto': Default hybrid mode. Loads into PostgreSQL if online, falling back
                      resiliently to DuckDB when running in offline/local environments.
        """
        self.settings = settings or get_settings()
        self.engine_mode = engine_mode.lower()
        self._local_db_path = duckdb_path or Path("data/quantaflow.duckdb")
        self.db_manager = DatabaseConnectionManager(
            settings=self.settings,
            engine_mode=self.engine_mode,
            duckdb_path=self._local_db_path,
        )
        self.pg_staging = PostgresStagingBulkLoader()

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

    def _bulk_upsert_postgres(
        self,
        conn: Any,
        df: pl.DataFrame,
        table_name: str,
        columns: list[str],
        conflict_keys: list[str],
        update_columns: list[str],
    ) -> None:
        """Delegate PostgreSQL bulk upsert to the specialized staging bulk loader."""
        self.pg_staging.bulk_upsert(
            conn=conn,
            df=df,
            table_name=table_name,
            columns=columns,
            conflict_keys=conflict_keys,
            update_columns=update_columns,
        )

    def _load_table_pipeline(
        self,
        df: pl.DataFrame,
        table_name: str,
        columns: list[str],
        conflict_keys: list[str],
        update_columns: list[str],
        duckdb_ddl: str,
        duckdb_upsert_sql: str,
    ) -> int:
        """Execute dual-target loading into PostgreSQL and DuckDB."""
        row_count = len(df)
        if row_count == 0:
            return 0

        # 1. Target: PostgreSQL
        conn = self._get_postgres_conn()
        if conn:
            try:
                self._bulk_upsert_postgres(
                    conn=conn,
                    df=df,
                    table_name=table_name,
                    columns=columns,
                    conflict_keys=conflict_keys,
                    update_columns=update_columns,
                )
                conn.commit()
                logger.info(f"postgres_{table_name}_loaded", count=row_count)
            except PG_ERRORS as exc:
                capture_pipeline_exception(exc, {"table": table_name, "layer": "warehouse"})
                logger.debug("postgres_load_fallback", table=table_name, error=str(exc))
            finally:
                conn.close()

        # 2. Target: DuckDB
        con = self._get_duckdb_conn()
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")
            con.execute(duckdb_ddl)
            con.register("tmp_df", df.to_arrow())
            con.execute(duckdb_upsert_sql)
        finally:
            con.close()

        logger.info(f"duckdb_{table_name}_loaded", count=row_count)
        return row_count

    def load_hourly_trip_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        ddl = """
            CREATE TABLE IF NOT EXISTS warehouse.hourly_trip_metrics (
                trip_hour TIMESTAMP PRIMARY KEY,
                trip_count BIGINT,
                total_distance DOUBLE,
                total_fare DOUBLE,
                avg_fare DOUBLE,
                avg_distance DOUBLE,
                avg_trip_duration DOUBLE,
                avg_passenger_count DOUBLE,
                source_period VARCHAR
            );
        """
        upsert_sql = """
            INSERT INTO warehouse.hourly_trip_metrics
            SELECT * FROM tmp_df
            ON CONFLICT (trip_hour) DO UPDATE SET
                trip_count = EXCLUDED.trip_count,
                total_distance = EXCLUDED.total_distance,
                total_fare = EXCLUDED.total_fare,
                avg_fare = EXCLUDED.avg_fare,
                avg_distance = EXCLUDED.avg_distance,
                avg_trip_duration = EXCLUDED.avg_trip_duration,
                avg_passenger_count = EXCLUDED.avg_passenger_count,
                source_period = EXCLUDED.source_period;
        """
        cols = [
            "trip_hour", "trip_count", "total_distance", "total_fare",
            "avg_fare", "avg_distance", "avg_trip_duration", "avg_passenger_count", "source_period",
        ]
        return self._load_table_pipeline(
            df=df,
            table_name="hourly_trip_metrics",
            columns=cols,
            conflict_keys=["trip_hour"],
            update_columns=cols[1:],
            duckdb_ddl=ddl,
            duckdb_upsert_sql=upsert_sql,
        )

    def load_zone_daily_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        ddl = """
            CREATE TABLE IF NOT EXISTS warehouse.zone_daily_metrics (
                trip_date DATE,
                location_id INTEGER,
                trip_count BIGINT,
                total_fare DOUBLE,
                avg_distance DOUBLE,
                avg_duration DOUBLE,
                source_period VARCHAR,
                PRIMARY KEY (trip_date, location_id)
            );
        """
        upsert_sql = """
            INSERT INTO warehouse.zone_daily_metrics
            SELECT * FROM tmp_df
            ON CONFLICT (trip_date, location_id) DO UPDATE SET
                trip_count = EXCLUDED.trip_count,
                total_fare = EXCLUDED.total_fare,
                avg_distance = EXCLUDED.avg_distance,
                avg_duration = EXCLUDED.avg_duration,
                source_period = EXCLUDED.source_period;
        """
        cols = [
            "trip_date", "location_id", "trip_count", "total_fare",
            "avg_distance", "avg_duration", "source_period",
        ]
        return self._load_table_pipeline(
            df=df,
            table_name="zone_daily_metrics",
            columns=cols,
            conflict_keys=["trip_date", "location_id"],
            update_columns=cols[2:],
            duckdb_ddl=ddl,
            duckdb_upsert_sql=upsert_sql,
        )

    def load_weather_impact_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        ddl = """
            CREATE TABLE IF NOT EXISTS warehouse.weather_impact_metrics (
                weather_hour TIMESTAMP PRIMARY KEY,
                trip_count BIGINT,
                avg_trip_distance DOUBLE,
                avg_trip_duration DOUBLE,
                avg_total_amount DOUBLE,
                precipitation DOUBLE,
                temperature DOUBLE,
                wind_speed DOUBLE,
                source_period VARCHAR
            );
        """
        upsert_sql = """
            INSERT INTO warehouse.weather_impact_metrics
            SELECT * FROM tmp_df
            ON CONFLICT (weather_hour) DO UPDATE SET
                trip_count = EXCLUDED.trip_count,
                avg_trip_distance = EXCLUDED.avg_trip_distance,
                avg_trip_duration = EXCLUDED.avg_trip_duration,
                avg_total_amount = EXCLUDED.avg_total_amount,
                precipitation = EXCLUDED.precipitation,
                temperature = EXCLUDED.temperature,
                wind_speed = EXCLUDED.wind_speed,
                source_period = EXCLUDED.source_period;
        """
        cols = [
            "weather_hour", "trip_count", "avg_trip_distance", "avg_trip_duration",
            "avg_total_amount", "precipitation", "temperature", "wind_speed", "source_period",
        ]
        return self._load_table_pipeline(
            df=df,
            table_name="weather_impact_metrics",
            columns=cols,
            conflict_keys=["weather_hour"],
            update_columns=cols[1:],
            duckdb_ddl=ddl,
            duckdb_upsert_sql=upsert_sql,
        )

    def load_daily_system_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        ddl = """
            CREATE TABLE IF NOT EXISTS warehouse.daily_system_metrics (
                run_date DATE PRIMARY KEY,
                rows_ingested BIGINT,
                rows_valid BIGINT,
                rows_rejected BIGINT,
                files_processed INTEGER,
                processing_time_seconds DOUBLE
            );
        """
        upsert_sql = """
            INSERT INTO warehouse.daily_system_metrics
            SELECT * FROM tmp_df
            ON CONFLICT (run_date) DO UPDATE SET
                rows_ingested = EXCLUDED.rows_ingested,
                rows_valid = EXCLUDED.rows_valid,
                rows_rejected = EXCLUDED.rows_rejected,
                files_processed = EXCLUDED.files_processed,
                processing_time_seconds = EXCLUDED.processing_time_seconds;
        """
        cols = [
            "run_date", "rows_ingested", "rows_valid",
            "rows_rejected", "files_processed", "processing_time_seconds",
        ]
        return self._load_table_pipeline(
            df=df,
            table_name="daily_system_metrics",
            columns=cols,
            conflict_keys=["run_date"],
            update_columns=cols[1:],
            duckdb_ddl=ddl,
            duckdb_upsert_sql=upsert_sql,
        )
