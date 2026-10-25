from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from src.common.errors import WarehouseConnectionError
from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

try:
    import psycopg2

    PG_ERRORS: tuple[type[Exception], ...] = (psycopg2.Error, OSError, ConnectionError)
except ImportError:  # pragma: no cover
    PG_ERRORS = (OSError, ConnectionError)

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
        self.local_db_path = duckdb_path or Path("data/quantaflow.duckdb")
        self.local_db_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_postgres_conn(self) -> Any:
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
                    f"Target host {self.settings.postgres_host}:{self.settings.postgres_port}. "
                    "Ensure PostgreSQL is running, or use engine_mode='duckdb' / 'auto'."
                ) from exc
            logger.info(
                "postgres_offline_fallback_active",
                detail="PostgreSQL unavailable; using local DuckDB warehouse engine.",
            )
            return None

    def _get_duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        try:
            return duckdb.connect(str(self.local_db_path))
        except (duckdb.Error, OSError, RuntimeError) as exc:
            raise WarehouseConnectionError(
                f"DuckDB warehouse connection failed at {self.local_db_path}: {exc}"
            ) from exc

    def load_hourly_trip_metrics(self, parquet_path: Path) -> int:
        df = pl.read_parquet(parquet_path)
        row_count = len(df)
        if row_count == 0:
            return 0

        # Try Postgres first
        conn = self._get_postgres_conn()
        if conn:
            try:
                with conn.cursor() as cur:
                    for row in df.iter_rows(named=True):
                        cur.execute(
                            """
                            INSERT INTO warehouse.hourly_trip_metrics (
                                trip_hour, trip_count, total_distance, total_fare,
                                avg_fare, avg_distance, avg_trip_duration,
                                avg_passenger_count, source_period
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (trip_hour) DO UPDATE SET
                                trip_count = EXCLUDED.trip_count,
                                total_distance = EXCLUDED.total_distance,
                                total_fare = EXCLUDED.total_fare,
                                avg_fare = EXCLUDED.avg_fare,
                                avg_distance = EXCLUDED.avg_distance,
                                avg_trip_duration = EXCLUDED.avg_trip_duration,
                                avg_passenger_count = EXCLUDED.avg_passenger_count,
                                source_period = EXCLUDED.source_period;
                            """,
                            (
                                row["trip_hour"],
                                row["trip_count"],
                                row["total_distance"],
                                row["total_fare"],
                                row["avg_fare"],
                                row["avg_distance"],
                                row["avg_trip_duration"],
                                row["avg_passenger_count"],
                                row["source_period"],
                            ),
                        )
                conn.commit()
                logger.info("postgres_hourly_trip_metrics_loaded", count=row_count)
            except PG_ERRORS as exc:
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
                with conn.cursor() as cur:
                    for row in df.iter_rows(named=True):
                        cur.execute(
                            """
                            INSERT INTO warehouse.zone_daily_metrics (
                                trip_date, location_id, trip_count, total_fare,
                                avg_distance, avg_duration, source_period
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (trip_date, location_id) DO UPDATE SET
                                trip_count = EXCLUDED.trip_count,
                                total_fare = EXCLUDED.total_fare,
                                avg_distance = EXCLUDED.avg_distance,
                                avg_duration = EXCLUDED.avg_duration,
                                source_period = EXCLUDED.source_period;
                            """,
                            (
                                row["trip_date"],
                                row["location_id"],
                                row["trip_count"],
                                row["total_fare"],
                                row["avg_distance"],
                                row["avg_duration"],
                                row["source_period"],
                            ),
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
                with conn.cursor() as cur:
                    for row in df.iter_rows(named=True):
                        cur.execute(
                            """
                            INSERT INTO warehouse.weather_impact_metrics (
                                weather_hour, trip_count, avg_trip_distance,
                                avg_trip_duration, avg_total_amount, precipitation,
                                temperature, wind_speed, source_period
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (weather_hour) DO UPDATE SET
                                trip_count = EXCLUDED.trip_count,
                                avg_trip_distance = EXCLUDED.avg_trip_distance,
                                avg_trip_duration = EXCLUDED.avg_trip_duration,
                                avg_total_amount = EXCLUDED.avg_total_amount,
                                precipitation = EXCLUDED.precipitation,
                                temperature = EXCLUDED.temperature,
                                wind_speed = EXCLUDED.wind_speed,
                                source_period = EXCLUDED.source_period;
                            """,
                            (
                                row["weather_hour"],
                                row["trip_count"],
                                row["avg_trip_distance"],
                                row["avg_trip_duration"],
                                row["avg_total_amount"],
                                row["precipitation"],
                                row["temperature"],
                                row["wind_speed"],
                                row["source_period"],
                            ),
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
                with conn.cursor() as cur:
                    for row in df.iter_rows(named=True):
                        cur.execute(
                            """
                            INSERT INTO warehouse.daily_system_metrics (
                                run_date, rows_ingested, rows_valid, rows_rejected,
                                files_processed, processing_time_seconds
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (run_date) DO UPDATE SET
                                rows_ingested = EXCLUDED.rows_ingested,
                                rows_valid = EXCLUDED.rows_valid,
                                rows_rejected = EXCLUDED.rows_rejected,
                                files_processed = EXCLUDED.files_processed,
                                processing_time_seconds = EXCLUDED.processing_time_seconds;
                            """,
                            (
                                row["run_date"],
                                row["rows_ingested"],
                                row["rows_valid"],
                                row["rows_rejected"],
                                row["files_processed"],
                                row["processing_time_seconds"],
                            ),
                        )
                conn.commit()
                logger.info("postgres_daily_system_metrics_loaded", count=row_count)
            except PG_ERRORS as exc:
                logger.debug("postgres_load_fallback", table="daily_system_metrics", error=str(exc))
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
