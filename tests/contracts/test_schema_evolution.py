"""Unit tests verifying SchemaEvolutionValidator backward, forward, and full compatibility."""

from pathlib import Path

import polars as pl
import pytest

from src.contracts.evolution import SchemaEvolutionValidator

CONTRACTS_DIR = Path(__file__).parent.parent.parent / "src" / "contracts"


@pytest.fixture
def sample_contract_spec() -> dict:
    return {
        "dataset": "test_dataset",
        "columns": [
            {"name": "id", "type": "int32", "required": True},
            {"name": "fare", "type": "float64", "required": True},
            {"name": "notes", "type": "string", "required": False},
        ],
    }


def test_backward_compatibility_safe_column_addition(sample_contract_spec: dict) -> None:
    """Verify adding new optional columns is backward compatible."""
    validator = SchemaEvolutionValidator()
    incoming = {
        "id": "int32",
        "fare": "float64",
        "notes": "string",
        "extra_col": "string",
    }
    report = validator.compare_schemas(incoming, sample_contract_spec, mode="BACKWARD")
    assert report.is_compatible is True
    assert "extra_col" in report.added_columns
    assert len(report.breaking_changes) == 0


def test_backward_compatibility_safe_widening(sample_contract_spec: dict) -> None:
    """Verify int32 -> int64 widening is accepted as safe evolution."""
    validator = SchemaEvolutionValidator()
    incoming = {
        "id": "int64",
        "fare": "float64",
        "notes": "string",
    }
    report = validator.compare_schemas(incoming, sample_contract_spec, mode="BACKWARD")
    assert report.is_compatible is True
    assert any("Safe type widening" in s for s in report.safe_evolutions)


def test_backward_compatibility_breaking_required_removal(sample_contract_spec: dict) -> None:
    """Verify removing a required column breaks backward compatibility."""
    validator = SchemaEvolutionValidator()
    incoming = {
        "fare": "float64",
        "notes": "string",
    }
    report = validator.compare_schemas(incoming, sample_contract_spec, mode="BACKWARD")
    assert report.is_compatible is False
    assert any("Removed required contract column 'id'" in b for b in report.breaking_changes)


def test_backward_compatibility_breaking_type_narrowing(sample_contract_spec: dict) -> None:
    """Verify float64 -> string or incompatible change breaks compatibility."""
    validator = SchemaEvolutionValidator()
    incoming = {
        "id": "int32",
        "fare": "string",  # breaking type
        "notes": "string",
    }
    report = validator.compare_schemas(incoming, sample_contract_spec, mode="BACKWARD")
    assert report.is_compatible is False
    assert any("Incompatible type change" in b for b in report.breaking_changes)


def test_forward_compatibility_rejects_new_columns(sample_contract_spec: dict) -> None:
    """Verify FORWARD compatibility mode rejects new columns not in contract."""
    validator = SchemaEvolutionValidator()
    incoming = {
        "id": "int32",
        "fare": "float64",
        "notes": "string",
        "new_field": "int32",
    }
    report = validator.compare_schemas(incoming, sample_contract_spec, mode="FORWARD")
    assert report.is_compatible is False
    assert any("FORWARD" in b for b in report.breaking_changes)


def test_validate_dataframe_against_actual_contract() -> None:
    """Verify validate_dataframe works against live taxi_zone_schema.yaml."""
    validator = SchemaEvolutionValidator()
    contract_file = CONTRACTS_DIR / "taxi_zone_schema.yaml"
    assert contract_file.is_file()

    df = pl.DataFrame({
        "LocationID": [1, 2],
        "Borough": ["Manhattan", "Queens"],
        "Zone": ["Central Park", "JFK Airport"],
        "service_zone": ["Yellow Zone", "Airports"],
    })
    report = validator.validate_dataframe(df, contract_file, mode="BACKWARD")
    assert report.is_compatible is True
