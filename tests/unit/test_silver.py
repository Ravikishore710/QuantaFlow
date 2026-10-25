from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from src.transformation.quarantine import QuarantineStore
from src.transformation.silver import SilverTransformer


def test_silver_clean_and_transform_trips(tmp_path: Path) -> None:
    bronze_trips = tmp_path / "bronze_trips.parquet"
    # Row 1: Valid
    # Row 2: Inverted timestamps (quarantine - base rule)
    # Row 3: Negative distance (quarantine - base rule)
    # Row 4: Duplicate of Row 1
    pl.DataFrame(
        {
            "tpep_pickup_datetime": [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 10, 30, 0),
                datetime(2024, 1, 1, 11, 0, 0),
                datetime(2024, 1, 1, 10, 0, 0),
            ],
            "tpep_dropoff_datetime": [
                datetime(2024, 1, 1, 10, 15, 0),
                datetime(2024, 1, 1, 10, 0, 0),  # invalid: dropoff before pickup
                datetime(2024, 1, 1, 11, 20, 0),
                datetime(2024, 1, 1, 10, 15, 0),
            ],
            "PULocationID": [10, 10, 10, 10],
            "DOLocationID": [20, 20, 20, 20],
            "trip_distance": [2.5, 3.0, -1.0, 2.5],  # Row 3 invalid: negative distance
            "passenger_count": [1, 1, 1, 1],
            "fare_amount": [12.0, 15.0, 10.0, 12.0],
            "total_amount": [15.0, 18.0, 12.0, 15.0],
        }
    ).write_parquet(bronze_trips)

    quarantine = QuarantineStore(quarantine_dir=tmp_path / "quarantine")
    transformer = SilverTransformer(quarantine_store=quarantine)
    transformer.silver_dir = tmp_path / "silver"

    target_path = transformer.clean_and_transform_trips(bronze_trips, "2024-01")
    assert target_path.exists()

    df = pl.read_parquet(target_path)
    # Only 1 distinct valid row should survive
    assert len(df) == 1
    assert "pickup_date" in df.columns
    assert "pickup_hour" in df.columns
    assert "trip_duration_seconds" in df.columns
    assert df["trip_duration_seconds"][0] == 900.0  # 15 minutes = 900 seconds

    # Check that quarantine captured the rejected rows
    rejected_count = quarantine.get_rejected_count("nyc_tlc_yellow", "2024-01")
    assert rejected_count == 2


def test_silver_physics_outlier_bounds_and_quarantine(tmp_path: Path) -> None:
    bronze_trips = tmp_path / "bronze_outliers.parquet"
    # Row 1: Valid normal ride
    # Row 2: Valid upper boundary ride (500 miles, $10,000 fare, 24 hr duration)
    # Row 3: Outlier trip distance (99999.0 miles)
    # Row 4: Outlier fare amount ($50,000.0)
    # Row 5: Outlier duration (> 24 hours: 26 hours)
    pickup_base = datetime(2024, 1, 1, 8, 0, 0)
    pl.DataFrame(
        {
            "tpep_pickup_datetime": [
                pickup_base,
                pickup_base,
                pickup_base,
                pickup_base,
                pickup_base,
            ],
            "tpep_dropoff_datetime": [
                pickup_base + timedelta(minutes=30),
                pickup_base + timedelta(hours=24),  # Exactly 24 hours (boundary valid)
                pickup_base + timedelta(hours=2),
                pickup_base + timedelta(hours=1),
                pickup_base + timedelta(hours=26),  # 26 hours (outlier)
            ],
            "PULocationID": [10, 11, 12, 13, 14],
            "DOLocationID": [20, 21, 22, 23, 24],
            "trip_distance": [3.5, 500.0, 99999.0, 15.0, 20.0],
            "passenger_count": [1, 2, 1, 1, 1],
            "fare_amount": [15.0, 10000.0, 50.0, 50000.0, 60.0],
            "total_amount": [18.0, 10000.0, 55.0, 50000.0, 65.0],
        }
    ).write_parquet(bronze_trips)

    quarantine = QuarantineStore(quarantine_dir=tmp_path / "quarantine")
    transformer = SilverTransformer(
        quarantine_store=quarantine,
        max_trip_distance=500.0,
        max_fare_amount=10000.0,
        max_trip_duration_minutes=1440.0,
    )
    transformer.silver_dir = tmp_path / "silver"

    target_path = transformer.clean_and_transform_trips(bronze_trips, "2024-01")
    assert target_path.exists()

    clean_df = pl.read_parquet(target_path)
    # Exactly Row 1 and Row 2 should pass
    assert len(clean_df) == 2
    assert set(clean_df["PULocationID"].to_list()) == {10, 11}

    # 3 outlier rows should be routed to quarantine
    quarantine_files = list((tmp_path / "quarantine").rglob("*.jsonl"))
    assert len(quarantine_files) > 0

    all_quarantine_lines = []
    for qf in quarantine_files:
        all_quarantine_lines.extend(qf.read_text(encoding="utf-8").strip().splitlines())

    assert len(all_quarantine_lines) == 3
    assert any("physics_outlier_violation" in line for line in all_quarantine_lines)
