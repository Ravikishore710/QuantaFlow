from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.transformation.gold import GoldTransformer


def test_gold_hourly_trip_metrics(tmp_path: Path) -> None:
    silver_path = tmp_path / "silver_trip_weather.parquet"
    pl.DataFrame(
        {
            "pickup_hour": [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 11, 0, 0),
            ],
            "pickup_date": [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 1, 1),
            ],
            "PULocationID": [10, 10, 20],
            "trip_distance": [2.0, 4.0, 3.0],
            "fare_amount": [10.0, 20.0, 15.0],
            "trip_duration_seconds": [600.0, 1200.0, 900.0],
            "passenger_count": [1, 2, 1],
            "total_amount": [12.0, 24.0, 18.0],
            "precipitation": [0.0, 0.0, 1.2],
            "temperature": [5.0, 5.0, 4.0],
            "wind_speed": [2.0, 2.0, 3.5],
        }
    ).write_parquet(silver_path)

    gold = GoldTransformer()
    gold.gold_dir = tmp_path / "gold"

    hourly_path = gold.build_hourly_trip_metrics(silver_path, "2024-01")
    assert hourly_path.exists()
    df_hourly = pl.read_parquet(hourly_path)
    assert len(df_hourly) == 2
    row_10 = df_hourly.filter(pl.col("trip_hour") == datetime(2024, 1, 1, 10, 0, 0))
    assert row_10["trip_count"][0] == 2
    assert row_10["total_fare"][0] == 30.0
    assert row_10["avg_fare"][0] == 15.0

    zone_path = gold.build_zone_daily_metrics(silver_path, "2024-01")
    assert zone_path.exists()
    df_zone = pl.read_parquet(zone_path)
    assert len(df_zone) == 2

    weather_path = gold.build_weather_impact_metrics(silver_path, "2024-01")
    assert weather_path.exists()
    df_weather = pl.read_parquet(weather_path)
    assert len(df_weather) == 2
