from pathlib import Path

import yaml


def load_contract(path: str | Path) -> dict:
    contract_path = Path(path)
    with contract_path.open("r") as handle:
        return yaml.safe_load(handle)


def validate_columns(
    actual_columns: list[str],
    contract: dict,
    allow_unexpected: bool = False,
) -> list[str]:
    errors: list[str] = []
    actual_set = set(actual_columns)
    required_columns = [col["name"] for col in contract["columns"] if col.get("required")]

    missing = [name for name in required_columns if name not in actual_set]
    if missing:
        errors.append(f"missing required columns: {missing}")

    if not allow_unexpected:
        known_columns = {col["name"] for col in contract["columns"]}
        unexpected = [name for name in actual_columns if name not in known_columns]
        if unexpected:
            errors.append(
                f"unexpected columns present, review schema evolution policy: {unexpected}"
            )

    return errors
