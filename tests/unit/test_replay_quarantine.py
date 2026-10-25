import json
from pathlib import Path

from scripts.replay_quarantine import is_record_valid, replay_quarantine


def test_is_record_valid_bounds() -> None:
    valid_rec = {
        "trip_distance": 3.5,
        "fare_amount": 15.0,
        "total_amount": 18.0,
        "tpep_pickup_datetime": "2024-01-01T10:00:00",
        "tpep_dropoff_datetime": "2024-01-01T10:20:00",
    }
    assert is_record_valid(valid_rec) is True

    # Upper bound distance violation (> 500)
    assert is_record_valid({**valid_rec, "trip_distance": 501.0}) is False

    # Upper bound fare violation (> 10000)
    assert is_record_valid({**valid_rec, "fare_amount": 10001.0}) is False

    # Negative total amount
    assert is_record_valid({**valid_rec, "total_amount": -5.0}) is False

    # Fare exceeds total amount
    assert is_record_valid({**valid_rec, "fare_amount": 20.0, "total_amount": 15.0}) is False

    # Inverted timestamps
    assert (
        is_record_valid(
            {
                **valid_rec,
                "tpep_pickup_datetime": "2024-01-01T11:00:00",
                "tpep_dropoff_datetime": "2024-01-01T10:00:00",
            }
        )
        is False
    )

    # Trip duration > 24 hours (1440 minutes)
    assert (
        is_record_valid(
            {
                **valid_rec,
                "tpep_pickup_datetime": "2024-01-01T10:00:00",
                "tpep_dropoff_datetime": "2024-01-02T11:00:00",
            }
        )
        is False
    )


def test_replay_quarantine_end_to_end(tmp_path: Path) -> None:
    quarantine_dir = tmp_path / "quarantine"
    silver_dir = tmp_path / "silver"

    period_dir = quarantine_dir / "nyc_tlc_yellow" / "period=2024-01"
    period_dir.mkdir(parents=True, exist_ok=True)

    records = [
        # Valid record that can be recovered
        {
            "raw_record": {
                "trip_distance": 2.5,
                "fare_amount": 12.0,
                "total_amount": 15.0,
                "tpep_pickup_datetime": "2024-01-01T12:00:00",
                "tpep_dropoff_datetime": "2024-01-01T12:15:00",
            }
        },
        # Record violating distance bound
        {
            "raw_record": {
                "trip_distance": 999.0,
                "fare_amount": 12.0,
                "total_amount": 15.0,
                "tpep_pickup_datetime": "2024-01-01T12:00:00",
                "tpep_dropoff_datetime": "2024-01-01T12:15:00",
            }
        },
        # Another valid record
        {
            "raw_record": {
                "trip_distance": 5.0,
                "fare_amount": 25.0,
                "total_amount": 30.0,
                "tpep_pickup_datetime": "2024-01-01T14:00:00",
                "tpep_dropoff_datetime": "2024-01-01T14:30:00",
            }
        },
    ]

    jsonl_file = period_dir / "quarantine_silver_test.jsonl"
    with jsonl_file.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
        f.write("corrupt-non-json-line\n")

    # 1. Test Dry Run mode
    dry_results = replay_quarantine(
        quarantine_dir=quarantine_dir,
        silver_dir=silver_dir,
        period="2024-01",
        dry_run=True,
    )
    assert dry_results["total_inspected"] == 4
    assert dry_results["recovered_rows"] == 2
    assert dry_results["still_invalid_rows"] == 2
    assert not (silver_dir / "trips" / "period=2024-01").exists()

    # 2. Test Live Recovery mode
    live_results = replay_quarantine(
        quarantine_dir=quarantine_dir,
        silver_dir=silver_dir,
        period="2024-01",
        dry_run=False,
    )
    assert live_results["recovered_rows"] == 2
    recovered_parquets = list((silver_dir / "trips" / "period=2024-01").glob("*.parquet"))
    assert len(recovered_parquets) == 1


def test_replay_quarantine_empty_dir(tmp_path: Path) -> None:
    empty_dir = tmp_path / "empty_quarantine"
    empty_dir.mkdir()
    res = replay_quarantine(empty_dir, tmp_path / "silver")
    assert res["total_inspected"] == 0
    assert res["recovered_rows"] == 0
    assert res["still_invalid_rows"] == 0
