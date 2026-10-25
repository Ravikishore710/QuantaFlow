"""Typed domain and infrastructure exceptions for QuantaFlow."""


class QuantaFlowError(Exception):
    """Base exception for all QuantaFlow errors."""

    pass


class WarehouseConnectionError(QuantaFlowError, ConnectionError):
    """Raised when the warehouse (PostgreSQL/DuckDB) fails to connect."""

    pass


class ManifestPersistenceError(QuantaFlowError, OSError):
    """Raised when saving or loading the file manifest fails."""

    pass


class DataQualityGateError(QuantaFlowError):
    """Raised when data quality checks fail the Write-Audit-Publish (WAP) circuit breaker."""

    def __init__(
        self,
        message: str,
        failed_checks: list[str] | None = None,
        measured_score: float | None = None,
        threshold: float | None = None,
    ) -> None:
        super().__init__(message)
        self.failed_checks = failed_checks or []
        self.measured_score = measured_score
        self.threshold = threshold

