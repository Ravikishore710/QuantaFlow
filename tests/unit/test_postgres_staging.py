"""Unit tests verifying PostgresStagingBulkLoader COPY staging and upsert builder."""

from unittest.mock import MagicMock

import polars as pl
import pytest

from src.warehouse.postgres_staging import PostgresStagingBulkLoader


def test_build_upsert_sql() -> None:
    """Verify generated INSERT ON CONFLICT SQL matches expected dialect syntax."""
    sql = PostgresStagingBulkLoader.build_upsert_sql(
        table_name="test_table",
        columns=["id", "val1", "val2"],
        conflict_keys=["id"],
        update_columns=["val1", "val2"],
        temp_table="tmp_test_table",
    )
    assert "INSERT INTO warehouse.test_table (id, val1, val2)" in sql
    assert "SELECT id, val1, val2 FROM tmp_test_table" in sql
    assert "ON CONFLICT (id) DO UPDATE SET" in sql
    assert "val1 = EXCLUDED.val1, val2 = EXCLUDED.val2" in sql


def test_bulk_upsert_empty_dataframe() -> None:
    """Verify empty DataFrame immediately returns 0 without database interaction."""
    loader = PostgresStagingBulkLoader()
    mock_conn = MagicMock()
    df = pl.DataFrame({"id": [], "val": []})

    loaded = loader.bulk_upsert(
        conn=mock_conn,
        df=df,
        table_name="dummy",
        columns=["id", "val"],
        conflict_keys=["id"],
        update_columns=["val"],
    )
    assert loaded == 0
    mock_conn.cursor.assert_not_called()


def test_bulk_upsert_with_copy_expert() -> None:
    """Verify bulk upsert executes COPY staging when copy_expert is available on cursor."""
    loader = PostgresStagingBulkLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    df = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})

    loaded = loader.bulk_upsert(
        conn=mock_conn,
        df=df,
        table_name="test_table",
        columns=["id", "val"],
        conflict_keys=["id"],
        update_columns=["val"],
    )
    assert loaded == 2
    assert mock_cursor.execute.call_count == 2  # CREATE TEMP TABLE and INSERT SELECT
    assert mock_cursor.copy_expert.call_count == 1

    first_call_sql = mock_cursor.execute.call_args_list[0][0][0]
    assert "CREATE TEMP TABLE tmp_test_table" in first_call_sql
    assert "ON COMMIT DROP" in first_call_sql

    copy_call_sql = mock_cursor.copy_expert.call_args[0][0]
    assert "COPY tmp_test_table (id, val) FROM STDIN" in copy_call_sql


def test_bulk_upsert_fallback_row_iteration() -> None:
    """Verify fallback to parameterized INSERT when cursor lacks copy_expert."""
    loader = PostgresStagingBulkLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock(spec=["execute"])  # Cursor without copy_expert
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    df = pl.DataFrame({"id": [10, 20], "val": ["x", "y"]})

    loaded = loader.bulk_upsert(
        conn=mock_conn,
        df=df,
        table_name="test_table",
        columns=["id", "val"],
        conflict_keys=["id"],
        update_columns=["val"],
    )
    assert loaded == 2
    assert mock_cursor.execute.call_count == 2


def test_bulk_upsert_exception_propagation() -> None:
    """Verify exceptions during COPY or staging are raised to caller."""
    loader = PostgresStagingBulkLoader()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.copy_expert.side_effect = RuntimeError("disk full on staging table")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    df = pl.DataFrame({"id": [1], "val": ["a"]})
    with pytest.raises(RuntimeError, match="disk full"):
        loader.bulk_upsert(
            conn=mock_conn,
            df=df,
            table_name="test_table",
            columns=["id", "val"],
            conflict_keys=["id"],
            update_columns=["val"],
        )
