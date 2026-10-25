import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl  # noqa: E402

from src.common.logging import configure_logging, get_logger  # noqa: E402
from src.config.settings import get_settings  # noqa: E402

logger = get_logger(__name__)


def is_record_valid(rec: dict[str, Any]) -> bool:
    """Validate a quarantined trip record against NYC TLC business and physics bounds."""
    try:
        dist = float(rec.get("trip_distance", -1))
        fare = float(rec.get("fare_amount", -1))
        total = float(rec.get("total_amount", -1))

        if not (0.0 <= dist <= 500.0):
            return False
        if not (0.0 <= fare <= 10000.0):
            return False
        if total < 0.0 or total < fare:
            return False

        pu_str = str(rec.get("tpep_pickup_datetime", ""))
        do_str = str(rec.get("tpep_dropoff_datetime", ""))
        if not pu_str or not do_str:
            return False

        # Parse timestamps (ISO or TLC format)
        pu_dt = datetime.fromisoformat(pu_str.replace("Z", "+00:00"))
        do_dt = datetime.fromisoformat(do_str.replace("Z", "+00:00"))

        if do_dt < pu_dt:
            return False

        duration_minutes = (do_dt - pu_dt).total_seconds() / 60.0
        return duration_minutes <= 1440.0
    except (ValueError, TypeError):
        return False


def replay_quarantine(
    quarantine_dir: Path,
    silver_dir: Path,
    period: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Scan quarantine files, re-validate records, and optionally recover valid rows to Silver."""
    quarantine_dir = Path(quarantine_dir)
    silver_dir = Path(silver_dir)

    search_dir = quarantine_dir
    if period:
        # Match both nested structure (dataset/period=YYYY-MM) and direct partition
        candidates = list(quarantine_dir.glob(f"**/period={period}"))
        if candidates:
            search_dir = candidates[0]

    jsonl_files = list(search_dir.glob("**/*.jsonl")) if search_dir.exists() else []

    total_inspected = 0
    recovered_records: list[dict[str, Any]] = []
    still_invalid = 0

    for jf in jsonl_files:
        with jf.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                total_inspected += 1
                try:
                    envelope = json.loads(line)
                    raw_record = envelope.get("raw_record", envelope)
                    if is_record_valid(raw_record):
                        recovered_records.append(raw_record)
                    else:
                        still_invalid += 1
                except json.JSONDecodeError:
                    still_invalid += 1

    recovered_count = len(recovered_records)
    logger.info(
        "quarantine_replay_scan_complete",
        total_inspected=total_inspected,
        recovered=recovered_count,
        still_invalid=still_invalid,
        dry_run=dry_run,
    )

    if not dry_run and recovered_count > 0:
        recovered_df = pl.DataFrame(recovered_records)
        target_period = period or "recovered"
        target_dir = silver_dir / "trips" / f"period={target_period}"
        target_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        target_path = target_dir / f"recovered_{ts}.parquet"
        recovered_df.write_parquet(target_path)
        logger.info("recovered_records_written_to_silver", path=str(target_path))

    return {
        "total_inspected": total_inspected,
        "recovered_rows": recovered_count,
        "still_invalid_rows": still_invalid,
    }


def main() -> None:
    configure_logging()
    settings = get_settings()

    parser = argparse.ArgumentParser(
        description="Replay Dead-Letter Queue (DLQ) quarantined records back into Silver"
    )
    parser.add_argument(
        "--quarantine-dir",
        type=Path,
        default=settings.quarantine_dir,
        help="Path to quarantine directory",
    )
    parser.add_argument(
        "--silver-dir",
        type=Path,
        default=settings.silver_dir,
        help="Path to silver directory",
    )
    parser.add_argument(
        "--period",
        type=str,
        default=None,
        help="Optional period filter (e.g., 2024-01)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Inspect and report without writing recovered records to Silver",
    )
    args = parser.parse_args()

    results = replay_quarantine(
        quarantine_dir=args.quarantine_dir,
        silver_dir=args.silver_dir,
        period=args.period,
        dry_run=args.dry_run,
    )
    print(
        f"Quarantine Replay Complete: Inspected={results['total_inspected']}, "
        f"Recovered={results['recovered_rows']}, Still Invalid={results['still_invalid_rows']}"
    )


if __name__ == "__main__":
    main()
