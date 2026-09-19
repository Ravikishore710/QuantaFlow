from datetime import datetime
from pathlib import Path

import polars as pl

from src.transformation.bronze import BronzeTransformer


def test_bronze_transform_zones(tmp_path: Path) -> None:
    raw_csv = tmp_path / "taxi_zone_lookup.csv"
    raw_csv.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,EWR,Newark Airport,EWR\n"
        "2,Queens,Jamaica Bay,Boro Zone\n"
    )

    transformer = BronzeTransformer()
    transformer.bronze_dir = tmp_path / "bronze"

    target_path = transformer.transform_zones(raw_csv)
    assert target_path.exists()

    df = pl.read_parquet(target_path)
    assert len(df) == 2
    assert "_ingested_at" in df.columns
    assert "_source_file" in df.columns
    assert df["_source_dataset"][0] == "nyc_tlc_taxi_zone"
    assert df["LocationID"].dtype == pl.Int64


def test_bronze_transform_trips(tmp_path: Path) -> None:
    raw_parquet = tmp_path / "raw_trips.parquet"
    pl.DataFrame(
        {
            "tpep_pickup_datetime": [datetime(2024, 1, 1, 10, 0, 0)],
            "tpep_dropoff_datetime": [datetime(2024, 1, 1, 10, 20, 0)],
            "PULocationID": [10],
            "DOLocationID": [20],
            "trip_distance": [2.5],
            "passenger_count": [1],
            "fare_amount": [15.0],
            "total_amount": [18.5],
        }
    ).write_parquet(raw_parquet)

    transformer = BronzeTransformer()
    transformer.bronze_dir = tmp_path / "bronze"

    target_path = transformer.transform_trips(raw_parquet, "2024-01")
    assert target_path.exists()

    df = pl.read_parquet(target_path)
    assert len(df) == 1
    assert df["_period"][0] == "2024-01"
    assert df["_source_dataset"][0] == "nyc_tlc_yellow"
    assert "_ingested_at" in df.columns
