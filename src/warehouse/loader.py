import io
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from src.common.db import PG_ERRORS, DatabaseConnectionManager
from src.common.error_tracking import capture_pipeline_exception
from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

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
            - 'postgres': Production target. Enforces PostgreSQL connection; raises
                          WarehouseConnectionError if PostgreSQL is unavailable.
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
        """High-throughput PostgreSQL bulk upsert using a temporary table and copy_expert."""
        with conn.cursor() as cur:
            if hasattr(cur, "copy_expert"):
                temp_table = f"tmp_{table_name}"
                cur.execute(
                    f"CREATE TEMP TABLE {temp_table} "
                    f"(LIKE warehouse.{table_name} INCLUDING DEFAULTS) ON COMMIT DROP;"
                )
                csv_buffer = io.StringIO()
                df.select(columns).write_csv(csv_buffer)
                csv_buffer.seek(0)
                cols_str = ", ".join(columns)
                copy_sql = f"COPY {temp_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, HEADER)"
                cur.copy_expert(copy_sql, csv_buffer)

                conflict_str = ", ".join(conflict_keys)
                update_set_str = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
                upsert_sql = (
                    f"INSERT INTO warehouse.{table_name} ({cols_str}) "
                    f"SELECT {cols_str} FROM {temp_table} "
                    f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_set_str};"
                )
                cur.execute(upsert_sql)
            else:
                cols_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                conflict_str = ", ".join(conflict_keys)
                update_set_str = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
                insert_sql = (
                    f"INSERT INTO warehouse.{table_name} ({cols_str}) "
                    f"VALUES ({placeholders}) "
                    f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_set_str};"
                )
                for row in df.iter_rows(named=True):
                    cur.execute(insert_sql, tuple(row[col] for col in columns))

    def load_hourly_trip_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        row_count = len(df)
        if row_count == 0:
            return 0

        # Try Postgres first
        conn = self._get_postgres_conn()
        if conn:
            try:
                self._bulk_upsert_postgres(
                    conn=conn,
                    df=df,
                    table_name="hourly_trip_metrics",
                    columns=[
                        "trip_hour",
                        "trip_count",
                        "total_distance",
                        "total_fare",
                        "avg_fare",
                        "avg_distance",
                        "avg_trip_duration",
                        "avg_passenger_count",
                        "source_period",
                    ],
                    conflict_keys=["trip_hour"],
                    update_columns=[
                        "trip_count",
                        "total_distance",
                        "total_fare",
                        "avg_fare",
                        "avg_distance",
                        "avg_trip_duration",
                        "avg_passenger_count",
                        "source_period",
                    ],
                )
                conn.commit()
                logger.info("postgres_hourly_trip_metrics_loaded", count=row_count)
            except PG_ERRORS as exc:
                capture_pipeline_exception(
                    exc, {"table": "hourly_trip_metrics", "layer": "warehouse"}
                )
                logger.debug("postgres_load_fallback", table="hourly_trip_metrics", error=str(exc))
            finally:
                conn.close()

        # Always maintain local DuckDB warehouse table for direct file SQL access
        con = self._get_duckdb_conn()
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")
            con.execute(
                """
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
            )
            con.register("tmp_df", df.to_arrow())
            con.execute(
                """
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
            )
        finally:
            con.close()

        logger.info("duckdb_hourly_trip_metrics_loaded", count=row_count)
        return row_count

    def load_zone_daily_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        row_count = len(df)
        if row_count == 0:
            return 0

        conn = self._get_postgres_conn()
        if conn:
            try:
                self._bulk_upsert_postgres(
                    conn=conn,
                    df=df,
                    table_name="zone_daily_metrics",
                    columns=[
                        "trip_date",
                        "location_id",
                        "trip_count",
                        "total_fare",
                        "avg_distance",
                        "avg_duration",
                        "source_period",
                    ],
                    conflict_keys=["trip_date", "location_id"],
                    update_columns=[
                        "trip_count",
                        "total_fare",
                        "avg_distance",
                        "avg_duration",
                        "source_period",
                    ],
                )
                conn.commit()
                logger.info("postgres_zone_daily_metrics_loaded", count=row_count)
            except PG_ERRORS as exc:
                logger.debug("postgres_load_fallback", table="zone_daily_metrics", error=str(exc))
            finally:
                conn.close()

        con = self._get_duckdb_conn()
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")
            con.execute(
                """
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
            )
            con.register("tmp_df", df.to_arrow())
            con.execute(
                """
                INSERT INTO warehouse.zone_daily_metrics
                SELECT * FROM tmp_df
                ON CONFLICT (trip_date, location_id) DO UPDATE SET
                    trip_count = EXCLUDED.trip_count,
                    total_fare = EXCLUDED.total_fare,
                    avg_distance = EXCLUDED.avg_distance,
                    avg_duration = EXCLUDED.avg_duration,
                    source_period = EXCLUDED.source_period;
                """
            )
        finally:
            con.close()

        logger.info("duckdb_zone_daily_metrics_loaded", count=row_count)
        return row_count

    def load_weather_impact_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        row_count = len(df)
        if row_count == 0:
            return 0

        conn = self._get_postgres_conn()
        if conn:
            try:
                self._bulk_upsert_postgres(
                    conn=conn,
                    df=df,
                    table_name="weather_impact_metrics",
                    columns=[
                        "weather_hour",
                        "trip_count",
                        "avg_trip_distance",
                        "avg_trip_duration",
                        "avg_total_amount",
                        "precipitation",
                        "temperature",
                        "wind_speed",
                        "source_period",
                    ],
                    conflict_keys=["weather_hour"],
                    update_columns=[
                        "trip_count",
                        "avg_trip_distance",
                        "avg_trip_duration",
                        "avg_total_amount",
                        "precipitation",
                        "temperature",
                        "wind_speed",
                        "source_period",
                    ],
                )
                conn.commit()
                logger.info("postgres_weather_impact_metrics_loaded", count=row_count)
            except PG_ERRORS as exc:
                logger.debug(
                    "postgres_load_fallback", table="weather_impact_metrics", error=str(exc)
                )
            finally:
                conn.close()

        con = self._get_duckdb_conn()
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")
            con.execute(
                """
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
            )
            con.register("tmp_df", df.to_arrow())
            con.execute(
                """
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
            )
        finally:
            con.close()

        logger.info("duckdb_weather_impact_metrics_loaded", count=row_count)
        return row_count

    def load_daily_system_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        row_count = len(df)
        if row_count == 0:
            return 0

        conn = self._get_postgres_conn()
        if conn:
            try:
                self._bulk_upsert_postgres(
                    conn=conn,
                    df=df,
                    table_name="daily_system_metrics",
                    columns=[
                        "run_date",
                        "rows_ingested",
                        "rows_valid",
                        "rows_rejected",
                        "files_processed",
                        "processing_time_seconds",
                    ],
                    conflict_keys=["run_date"],
                    update_columns=[
                        "rows_ingested",
                        "rows_valid",
                        "rows_rejected",
                        "files_processed",
                        "processing_time_seconds",
                    ],
                )
                conn.commit()
                logger.info("postgres_daily_system_metrics_loaded", count=row_count)
            except PG_ERRORS as exc:
                logger.debug(
                    "postgres_load_fallback", table="daily_system_metrics", error=str(exc)
                )
            finally:
                conn.close()

        con = self._get_duckdb_conn()
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS warehouse.daily_system_metrics (
                    run_date DATE PRIMARY KEY,
                    rows_ingested BIGINT,
                    rows_valid BIGINT,
                    rows_rejected BIGINT,
                    files_processed INTEGER,
                    processing_time_seconds DOUBLE
                );
                """
            )
            con.register("tmp_df", df.to_arrow())
            con.execute(
                """
                INSERT INTO warehouse.daily_system_metrics
                SELECT * FROM tmp_df
                ON CONFLICT (run_date) DO UPDATE SET
                    rows_ingested = EXCLUDED.rows_ingested,
                    rows_valid = EXCLUDED.rows_valid,
                    rows_rejected = EXCLUDED.rows_rejected,
                    files_processed = EXCLUDED.files_processed,
                    processing_time_seconds = EXCLUDED.processing_time_seconds;
                """
            )
        finally:
            con.close()

        logger.info("duckdb_daily_system_metrics_loaded", count=row_count)
        return row_count
