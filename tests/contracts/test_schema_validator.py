from pathlib import Path

from src.validation.schema_validator import load_contract, validate_columns

CONTRACT_PATH = Path("src/contracts/taxi_zone_schema.yaml")


def test_load_contract_reads_expected_dataset_name() -> None:
    contract = load_contract(CONTRACT_PATH)

    assert contract["dataset"] == "nyc_tlc_taxi_zone"
    assert len(contract["columns"]) == 4


def test_validate_columns_passes_for_exact_match() -> None:
    contract = load_contract(CONTRACT_PATH)
    columns = ["LocationID", "Borough", "Zone", "service_zone"]

    errors = validate_columns(columns, contract)

    assert errors == []


def test_validate_columns_flags_missing_required_column() -> None:
    contract = load_contract(CONTRACT_PATH)
    columns = ["LocationID", "Borough", "Zone"]

    errors = validate_columns(columns, contract)

    assert len(errors) == 1
    assert "service_zone" in errors[0]


def test_validate_columns_flags_unexpected_column() -> None:
    contract = load_contract(CONTRACT_PATH)
    columns = ["LocationID", "Borough", "Zone", "service_zone", "extra_column"]

    errors = validate_columns(columns, contract)

    assert len(errors) == 1
    assert "extra_column" in errors[0]
