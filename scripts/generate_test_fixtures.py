from datetime import datetime, timedelta
from pathlib import Path

import polars as pl


def generate_fixtures(output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. TLC Yellow Taxi Fixture (50 rows)
    base_pickup = datetime(2024, 1, 1, 6, 0, 0)
    tlc_rows = []
    for i in range(50):
        pu = base_pickup + timedelta(hours=i * 2, minutes=(i * 7) % 60)
        duration_minutes = 10 + (i * 3) % 45
        do = pu + timedelta(minutes=duration_minutes)
        dist = round(1.2 + (i * 0.45), 2)
        fare = round(8.0 + (dist * 2.8), 2)
        extra = 1.0 if pu.hour in (16, 17, 18, 19) else 0.5
        mta_tax = 0.5
        tip = round(fare * 0.18, 2)
        tolls = 0.0 if i % 5 != 0 else 6.55
        total = round(fare + extra + mta_tax + tip + tolls, 2)

        tlc_rows.append(
            {
                "VendorID": 1 if i % 2 == 0 else 2,
                "tpep_pickup_datetime": pu,
                "tpep_dropoff_datetime": do,
                "passenger_count": (i % 4) + 1,
                "trip_distance": dist,
                "RatecodeID": 1,
                "store_and_fwd_flag": "N",
                "PULocationID": (i % 20) + 1,
                "DOLocationID": ((i + 3) % 20) + 1,
                "payment_type": 1,
                "fare_amount": fare,
                "extra": extra,
                "mta_tax": mta_tax,
                "tip_amount": tip,
                "tolls_amount": tolls,
                "improvement_surcharge": 1.0,
                "total_amount": total,
                "congestion_surcharge": 2.5,
                "Airport_fee": 1.75 if (i % 10 == 0) else 0.0,
            }
        )

    tlc_df = pl.DataFrame(tlc_rows)
    tlc_path = output_dir / "tlc_yellow_2024-01.parquet"
    tlc_df.write_parquet(tlc_path)

    # 2. NOAA Weather Fixture (48 hourly rows)
    weather_base = datetime(2024, 1, 1, 0, 0, 0)
    weather_rows = []
    for h in range(48):
        ts = weather_base + timedelta(hours=h)
        temp_c = round(2.5 + (h % 8) * 0.6, 1)
        dew_c = round(temp_c - 3.2, 1)
        wind_ms = round(3.0 + (h % 5) * 0.7, 1)
        precip = 0.0 if h % 12 != 0 else 1.2
        vis_km = 16.0 if precip == 0.0 else 8.5
        cond = "Clear" if precip == 0.0 else "Light Rain"

        weather_rows.append(
            {
                "timestamp": ts,
                "station_id": "USW00094728",
                "latitude": 40.7789,
                "longitude": -73.9692,
                "temperature": temp_c,
                "dew_point": dew_c,
                "wind_speed": wind_ms,
                "precipitation": precip,
                "visibility": vis_km,
                "pressure": 1014.5,
                "weather_condition": cond,
            }
        )

    weather_df = pl.DataFrame(weather_rows)
    weather_path = output_dir / "noaa_ghcnh_2024-01.parquet"
    weather_df.write_parquet(weather_path)

    # 3. Taxi Zone Lookup Fixture (20 rows)
    zones = [
        (1, "EWR", "Newark Airport", "EWR"),
        (2, "Queens", "Jamaica Bay", "Boro Zone"),
        (3, "Bronx", "Allerton/Pelham Gardens", "Boro Zone"),
        (4, "Manhattan", "Alphabet City", "Yellow Zone"),
        (5, "Staten Island", "Arden Heights", "Boro Zone"),
        (6, "Staten Island", "Arrochar/Fort Wadsworth", "Boro Zone"),
        (7, "Queens", "Astoria", "Boro Zone"),
        (8, "Queens", "Astoria Park", "Boro Zone"),
        (9, "Queens", "Auburndale", "Boro Zone"),
        (10, "Queens", "Baisley Park", "Boro Zone"),
        (11, "Brooklyn", "Bath Beach", "Boro Zone"),
        (12, "Manhattan", "Battery Park", "Yellow Zone"),
        (13, "Manhattan", "Battery Park City", "Yellow Zone"),
        (14, "Brooklyn", "Bay Ridge", "Boro Zone"),
        (15, "Queens", "Bay Terrace/Fort Totten", "Boro Zone"),
        (16, "Queens", "Bayside", "Boro Zone"),
        (17, "Brooklyn", "Bedford", "Boro Zone"),
        (18, "Bronx", "Bedford Park", "Boro Zone"),
        (19, "Queens", "Bellerose", "Boro Zone"),
        (20, "Bronx", "Belmont", "Boro Zone"),
    ]
    zone_lines = ["LocationID,Borough,Zone,service_zone"]
    for zid, borough, zone_name, service in zones:
        zone_lines.append(f'{zid},{borough},"{zone_name}",{service}')

    zone_path = output_dir / "taxi_zone_lookup.csv"
    zone_path.write_text("\n".join(zone_lines) + "\n", encoding="utf-8")

    return {
        "tlc": tlc_path,
        "weather": weather_path,
        "zone": zone_path,
    }


def main() -> None:
    fixtures_dir = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
    paths = generate_fixtures(fixtures_dir)
    print(f"Generated fixtures under {fixtures_dir}:")
    for k, p in paths.items():
        print(f"  {k}: {p.name} ({p.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
