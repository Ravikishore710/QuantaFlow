"""PostgreSQL high-throughput COPY staging and bulk upsert manager."""

import io
from typing import Any

import polars as pl

from src.common.logging import get_logger

logger = get_logger(__name__)


class PostgresStagingBulkLoader:
    """Manages high-throughput PostgreSQL bulk upsert operations using temporary tables and COPY."""

    @staticmethod
    def build_upsert_sql(
        table_name: str,
        columns: list[str],
        conflict_keys: list[str],
        update_columns: list[str],
        temp_table: str,
    ) -> str:
        """Construct the INSERT ... SELECT ... ON CONFLICT DO UPDATE statement."""
        cols_str = ", ".join(columns)
        conflict_str = ", ".join(conflict_keys)
        update_set_str = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
        return (
            f"INSERT INTO warehouse.{table_name} ({cols_str}) "
            f"SELECT {cols_str} FROM {temp_table} "
            f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_set_str};"
        )

    def bulk_upsert(
        self,
        conn: Any,
        df: pl.DataFrame,
        table_name: str,
        columns: list[str],
        conflict_keys: list[str],
        update_columns: list[str],
    ) -> int:
        """Execute set-based bulk upsert via COPY streaming to temporary staging table."""
        if len(df) == 0:
            return 0

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

                upsert_sql = self.build_upsert_sql(
                    table_name=table_name,
                    columns=columns,
                    conflict_keys=conflict_keys,
                    update_columns=update_columns,
                    temp_table=temp_table,
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

        return len(df)
