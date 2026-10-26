import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.run_pipeline import run_pipeline  # noqa: E402
from src.common.logging import configure_logging, get_logger  # noqa: E402
from src.sources.synthetic import (  # noqa: E402
    generate_synthetic_taxi_data,
    generate_synthetic_weather_data,
)
from src.validation.ingress import BackfillExecutionRequest  # noqa: E402

logger = get_logger(__name__)


def generate_period_range(start_period: str, end_period: str) -> list[str]:
    start_dt = datetime.strptime(start_period, "%Y-%m")
    end_dt = datetime.strptime(end_period, "%Y-%m")

    periods = []
    curr = start_dt
    while curr <= end_dt:
        periods.append(curr.strftime("%Y-%m"))
        # Move to next month
        year = curr.year + (curr.month // 12)
        month = (curr.month % 12) + 1
        curr = datetime(year, month, 1)

    return periods


def run_backfill(
    start_period: str, end_period: str, use_synthetic: bool = False, skip_upload: bool = True
) -> int:
    req = BackfillExecutionRequest(
        start_period=start_period,
        end_period=end_period,
        use_synthetic=use_synthetic,
        skip_upload=skip_upload,
    )
    start_period = req.start_period
    end_period = req.end_period
    configure_logging()
    periods = generate_period_range(start_period, end_period)
    logger.info("backfill_started", periods=periods, use_synthetic=use_synthetic)

    t0 = time.time()
    results = []

    for period in periods:
        logger.info("backfill_processing_period", period=period)
        if use_synthetic and period != "2024-01":
            # Generate synthetic monthly sample for offline testing
            generate_synthetic_taxi_data(period, num_rows=10_000)
            generate_synthetic_weather_data(period)

        code = run_pipeline(period=period, skip_upload=skip_upload)
        results.append((period, code))
        if code != 0:
            logger.error("backfill_period_failed", period=period)
            return code

    total_time = round(time.time() - t0, 2)
    logger.info(
        "backfill_completed_successfully", total_periods=len(periods), duration_seconds=total_time
    )
    print("\n" + "=" * 60)
    print(f"QUANTAFLOW 6-MONTH BACKFILL SUMMARY ({start_period} to {end_period})")
    print("=" * 60)
    for p, c in results:
        status_str = "SUCCESS (0)" if c == 0 else f"FAILED ({c})"
        print(f"  Period {p}: {status_str}")
    print(f"Total Duration: {total_time}s")
    print("=" * 60 + "\n")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Execute QuantaFlow historical backfill across multiple periods"
    )
    parser.add_argument("--start", default="2024-01", help="Start period (YYYY-MM)")
    parser.add_argument("--end", default="2024-06", help="End period (YYYY-MM)")
    parser.add_argument(
        "--use-synthetic",
        action="store_true",
        default=False,
        help="Use fast synthetic data generation for testing 6 months offline",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        default=True,
        help="Skip MinIO/S3 upload and run locally",
    )
    args = parser.parse_args()
    sys.exit(
        run_backfill(
            args.start, args.end, use_synthetic=args.use_synthetic, skip_upload=args.skip_upload
        )
    )


if __name__ == "__main__":
    main()
