from datetime import datetime
from pathlib import Path

import polars as pl

from src.transformation.quarantine import QuarantineStore
from src.transformation.silver import SilverTransformer


def test_silver_clean_and_transform_trips(tmp_path: Path) -> None:
    bronze_trips = tmp_path / "bronze_trips.parquet"
    # Row 1: Valid
    # Row 2: Inverted timestamps (quarantine)
    # Row 3: Negative distance (quarantine)
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
