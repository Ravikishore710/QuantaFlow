"""Statistical anomaly detection for data engineering metrics using Median Absolute Deviation (MAD).

Detects anomalous volume, fare, distance, and quarantine rate deviations across historical runs.
"""

import math
from dataclasses import dataclass


@dataclass
class AnomalyCheckResult:
    """Evaluation result for a single metric against historical distribution baselines."""

    metric_name: str
    observed_value: float
    baseline_median: float
    z_score: float
    is_anomaly: bool
    reason: str


class StatisticalAnomalyDetector:
    """Robust statistical anomaly detector using Median Absolute Deviation (MAD) and IQR bounds."""

    @staticmethod
    def _median(values: list[float]) -> float:
        """Compute the median of a list of floats."""
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        if n == 0:
            return 0.0
        mid = n // 2
        if n % 2 == 1:
            return sorted_vals[mid]
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0

    @classmethod
    def compute_mad_z_score(cls, observed: float, baseline: list[float]) -> tuple[float, float]:
        """Calculate modified Z-score using Median Absolute Deviation (MAD).

        Returns
        -------
        tuple[float, float]
            (baseline_median, modified_z_score)
        """
        if not baseline:
            return observed, 0.0

        med = cls._median(baseline)
        deviations = [abs(x - med) for x in baseline]
        mad = cls._median(deviations)

        # Constant 0.6745 scales MAD to approximate standard deviation for normal distribution
        if mad > 1e-9:
            z_score = 0.6745 * (observed - med) / mad
        else:
            # Fallback to mean absolute deviation or simple relative diff if MAD is 0
            mean_val = sum(baseline) / len(baseline)
            variance = sum((x - mean_val) ** 2 for x in baseline) / len(baseline)
            std_dev = math.sqrt(variance)
            z_score = (observed - med) / std_dev if std_dev > 1e-9 else 0.0

        return med, round(z_score, 4)

    def evaluate_metric(
        self,
        metric_name: str,
        observed_value: float,
        baseline: list[float],
        z_threshold: float = 3.5,
    ) -> AnomalyCheckResult:
        """Evaluate whether a metric observation constitutes a statistical outlier."""
        if len(baseline) < 3:
            # Insufficient historical baseline data to infer statistical distribution
            return AnomalyCheckResult(
                metric_name=metric_name,
                observed_value=observed_value,
                baseline_median=observed_value,
                z_score=0.0,
                is_anomaly=False,
                reason="baseline_history_too_small",
            )

        med, z_score = self.compute_mad_z_score(observed_value, baseline)
        is_anomaly = abs(z_score) >= z_threshold

        reason = "normal"
        if is_anomaly:
            direction = "spike" if z_score > 0 else "drop"
            reason = (
                f"{direction}_detected: z_score={z_score:.2f} exceeds threshold {z_threshold} "
                f"(observed={observed_value:.2f}, baseline_median={med:.2f})"
            )

        return AnomalyCheckResult(
            metric_name=metric_name,
            observed_value=observed_value,
            baseline_median=med,
            z_score=z_score,
            is_anomaly=is_anomaly,
            reason=reason,
        )

    def evaluate_batch(
        self,
        metrics: dict[str, float],
        historical_baselines: dict[str, list[float]],
        z_threshold: float = 3.5,
    ) -> list[AnomalyCheckResult]:
        """Evaluate a dictionary of current run metrics against known historical series."""
        results: list[AnomalyCheckResult] = []
        for metric_name, observed_val in metrics.items():
            baseline = historical_baselines.get(metric_name, [])
            results.append(
                self.evaluate_metric(
                    metric_name=metric_name,
                    observed_value=observed_val,
                    baseline=baseline,
                    z_threshold=z_threshold,
                )
            )
        return results
