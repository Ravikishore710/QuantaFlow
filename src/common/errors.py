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
