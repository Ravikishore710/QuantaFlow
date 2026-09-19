from datetime import datetime, timedelta
from pathlib import Path

import polars as pl


def generate_synthetic_taxi_data(
    period: str, num_rows: int = 10_000, target_path: Path | None = None
) -> Path:
    year_str, month_str = period.split("-")
    year = int(year_str)
    month = int(month_str)

    base_time = datetime(year, month, 1, 0, 0, 0)
    pickup_times = [base_time + timedelta(minutes=i * 4) for i in range(num_rows)]
    dropoff_times = [t + timedelta(minutes=10 + (i % 20)) for i, t in enumerate(pickup_times)]

    pu_locs = [1 + (i % 263) for i in range(num_rows)]
    do_locs = [1 + ((i + 7) % 263) for i in range(num_rows)]
    distances = [round(1.0 + (i % 15) * 0.7, 2) for i in range(num_rows)]
    fares = [round(5.0 + d * 2.5, 2) for d in distances]
    tips = [round(f * 0.18, 2) for f in fares]
    totals = [round(f + t + 2.5, 2) for f, t in zip(fares, tips, strict=False)]

    df = pl.DataFrame(
        {
            "VendorID": [1 + (i % 2) for i in range(num_rows)],
            "tpep_pickup_datetime": pickup_times,
            "tpep_dropoff_datetime": dropoff_times,
            "passenger_count": [1 + (i % 4) for i in range(num_rows)],
            "trip_distance": distances,
            "RatecodeID": [1 for _ in range(num_rows)],
            "store_and_fwd_flag": ["N" for _ in range(num_rows)],
            "PULocationID": pu_locs,
            "DOLocationID": do_locs,
            "payment_type": [1 + (i % 2) for i in range(num_rows)],
            "fare_amount": fares,
            "extra": [0.5 for _ in range(num_rows)],
            "mta_tax": [0.5 for _ in range(num_rows)],
            "tip_amount": tips,
            "tolls_amount": [0.0 for _ in range(num_rows)],
            "improvement_surcharge": [1.0 for _ in range(num_rows)],
            "total_amount": totals,
            "congestion_surcharge": [2.5 for _ in range(num_rows)],
            "Airport_fee": [0.0 for _ in range(num_rows)],
        }
    )

    out_path = target_path or Path(f"data/samples/yellow_trip_{year_str}_{month_str}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    return out_path


def generate_synthetic_weather_data(
    period: str, station_id: str = "USW00094728", target_path: Path | None = None
) -> Path:
    year_str, month_str = period.split("-")
    year = int(year_str)
    month = int(month_str)

    base_time = datetime(year, month, 1, 0, 0, 0)
    # Approx 720 hours in a month
    num_hours = 744 if month in [1, 3, 5, 7, 8, 10, 12] else (720 if month != 2 else 696)
    hours = [base_time + timedelta(hours=i) for i in range(num_hours)]

    df = pl.DataFrame(
        {
            "timestamp": hours,
            "station_id": [station_id for _ in range(num_hours)],
            "latitude": [40.78 for _ in range(num_hours)],
            "longitude": [-73.97 for _ in range(num_hours)],
            "temperature": [round(2.0 + (i % 15) * 0.8, 1) for i in range(num_hours)],
            "dew_point": [1.0 for _ in range(num_hours)],
            "wind_speed": [round(3.0 + (i % 7) * 0.4, 1) for i in range(num_hours)],
            "precipitation": [round(0.5 * (i % 5 == 0), 2) for i in range(num_hours)],
            "visibility": [10.0 for _ in range(num_hours)],
            "pressure": [1013.25 for _ in range(num_hours)],
            "weather_condition": ["CLEAR" for _ in range(num_hours)],
        }
    )

    out_path = target_path or Path(
        f"data/samples/ghcnh_{station_id}_{year_str}_{month_str}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    return out_path
