import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import polars as pl

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


@dataclass
class QualityCheckResult:
    check_name: str
    dimension: str
    severity: str  # "error" or "warning"
    rows_checked: int
    violations: int
    status: str  # "passed" or "failed"
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class QualitySuiteReport:
    run_id: str
    dataset: str
    partition: str
    evaluated_at: str
    overall_status: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    checks: list[QualityCheckResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dataset": self.dataset,
            "partition": self.partition,
            "evaluated_at": self.evaluated_at,
            "overall_status": self.overall_status,
            "total_checks": self.total_checks,
            "passed_checks": self.passed_checks,
            "failed_checks": self.failed_checks,
            "checks": [c.to_dict() for c in self.checks],
        }


class DataQualityFramework:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.reports_dir = self.settings.reports_dir
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def evaluate_trips(
        self,
        df: pl.DataFrame,
        run_id: str,
        period: str,
        previous_row_count: int | None = None,
    ) -> QualitySuiteReport:
        """Evaluates 6 dimensions of data quality against clean trip data:
        1. Completeness: Zero NULLs across critical columns.
        2. Validity: Non-negative distance (>= 0), non-negative fare (>= 0),
           and chronological timestamps (dropoff >= pickup).
        3. Uniqueness: No duplicate records on compound business keys.
        4. Consistency: total_amount >= fare_amount.
        5. Freshness: pickup timestamps within target monthly partition.
        6. Volume: Deviation check against prior volume baselines.
        """
        rows_total = len(df)
        checks: list[QualityCheckResult] = []

        # 1. Completeness: NULLs in critical columns
        critical_cols = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "fare_amount",
        ]
        null_counts = {col: df[col].null_count() for col in critical_cols if col in df.columns}
        total_nulls = sum(null_counts.values())
        checks.append(
            QualityCheckResult(
                check_name="critical_columns_completeness",
                dimension="completeness",
                severity="error",
                rows_checked=rows_total,
                violations=total_nulls,
                status="passed" if total_nulls == 0 else "failed",
                details=null_counts,
            )
        )

        # 2. Validity: Negative values or dropoff before pickup
        invalid_dist = (
            len(df.filter(pl.col("trip_distance") < 0)) if "trip_distance" in df.columns else 0
        )
        invalid_fare = (
            len(df.filter(pl.col("fare_amount") < 0)) if "fare_amount" in df.columns else 0
        )
        invalid_times = (
            len(df.filter(pl.col("tpep_dropoff_datetime") < pl.col("tpep_pickup_datetime")))
            if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns
            else 0
        )
        validity_violations = invalid_dist + invalid_fare + invalid_times
        checks.append(
            QualityCheckResult(
                check_name="business_rules_validity",
                dimension="validity",
                severity="error",
                rows_checked=rows_total,
                violations=validity_violations,
                status="passed" if validity_violations == 0 else "failed",
                details={
                    "negative_distance": invalid_dist,
                    "negative_fare": invalid_fare,
                    "inverted_timestamps": invalid_times,
                },
            )
        )

        # 3. Uniqueness: Duplicate records
        dedup_keys = [
            col
            for col in [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "fare_amount",
            ]
            if col in df.columns
        ]
        duplicate_count = rows_total - df.select(dedup_keys).n_unique() if dedup_keys else 0
        checks.append(
            QualityCheckResult(
                check_name="duplicate_trips_uniqueness",
                dimension="uniqueness",
                severity="warning",
                rows_checked=rows_total,
                violations=duplicate_count,
                status="passed" if duplicate_count == 0 else "failed",
                details={"duplicate_records": duplicate_count},
            )
        )

        # 4. Consistency: total_amount >= fare_amount
        if "total_amount" in df.columns and "fare_amount" in df.columns:
            inconsistent_count = len(df.filter(pl.col("total_amount") < pl.col("fare_amount")))
        else:
            inconsistent_count = 0
        checks.append(
            QualityCheckResult(
                check_name="fare_total_consistency",
                dimension="consistency",
                severity="warning",
                rows_checked=rows_total,
                violations=inconsistent_count,
                status="passed" if inconsistent_count == 0 else "failed",
                details={"total_less_than_fare": inconsistent_count},
            )
        )

        # 5. Freshness: Timestamps in bounds of period
        if "tpep_pickup_datetime" in df.columns and rows_total > 0:
            year, month = period.split("-")
            out_of_month = len(
                df.filter(
                    (pl.col("tpep_pickup_datetime").dt.year() != int(year))
                    | (pl.col("tpep_pickup_datetime").dt.month() != int(month))
                )
            )
        else:
            out_of_month = 0
        checks.append(
            QualityCheckResult(
                check_name="pickup_timestamp_period_freshness",
                dimension="freshness",
                severity="warning",
                rows_checked=rows_total,
                violations=out_of_month,
                status="passed" if out_of_month == 0 else "failed",
                details={"records_outside_period": out_of_month},
            )
        )

        # 6. Volume: Anomaly check against previous row count
        volume_violations = 0
        vol_details: dict[str, Any] = {"current_rows": rows_total}
        if previous_row_count is not None and previous_row_count > 0:
            pct_change = abs(rows_total - previous_row_count) / previous_row_count
            vol_details["previous_rows"] = previous_row_count
            vol_details["pct_change"] = round(pct_change, 4)
            if pct_change > self.settings.row_count_change_threshold:
                volume_violations = 1
        checks.append(
            QualityCheckResult(
                check_name="row_count_volume_anomaly",
                dimension="volume",
                severity="warning",
                rows_checked=rows_total,
                violations=volume_violations,
                status="passed" if volume_violations == 0 else "failed",
                details=vol_details,
            )
        )

        failed_errors = sum(1 for c in checks if c.status == "failed" and c.severity == "error")
        overall_status = "failed" if failed_errors > 0 else "passed"
        now_iso = datetime.now(UTC).isoformat()

        report = QualitySuiteReport(
            run_id=run_id,
            dataset="nyc_tlc_yellow",
            partition=period,
            evaluated_at=now_iso,
            overall_status=overall_status,
            total_checks=len(checks),
            passed_checks=sum(1 for c in checks if c.status == "passed"),
            failed_checks=sum(1 for c in checks if c.status == "failed"),
            checks=checks,
        )

        # Save machine-readable JSON report
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.reports_dir / f"quality_report_{period}_{run_id}.json"
        report_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
        logger.info(
            "quality_evaluation_complete",
            run_id=run_id,
            dataset="nyc_tlc_yellow",
            period=period,
            status=overall_status,
            report=str(report_path),
        )
        return report
