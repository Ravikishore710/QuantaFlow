"""Schema evolution and backward-compatibility validator.

Evaluates schema modifications against registered YAML contracts using BACKWARD, FORWARD,
and FULL compatibility rules.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import yaml


@dataclass
class SchemaDiffReport:
    """Detailed report capturing schema changes and compatibility verdicts."""

    is_compatible: bool
    compatibility_mode: str
    breaking_changes: list[str]
    safe_evolutions: list[str]
    added_columns: list[str]
    removed_columns: list[str]
    type_changes: list[str]


# Permissible type widening conversions that preserve precision
SAFE_TYPE_WIDENING: dict[str, set[str]] = {
    "int32": {"int64", "float64"},
    "int64": {"float64"},
    "float32": {"float64"},
    "date": {"datetime"},
}


class SchemaEvolutionValidator:
    """Validates schema evolution compatibility across Medallion boundaries."""

    @staticmethod
    def _normalize_type(type_str: str) -> str:
        """Normalize type string to lowercase standard identifier."""
        t = type_str.lower().strip()
        if "datetime" in t or "timestamp" in t:
            return "datetime"
        if "int64" in t or "bigint" in t:
            return "int64"
        if "int" in t or "integer" in t:
            return "int32"
        if "float" in t or "double" in t:
            return "float64"
        if "str" in t or "string" in t or "varchar" in t or "utf8" in t:
            return "string"
        if "bool" in t:
            return "boolean"
        if "date" in t:
            return "date"
        return t

    @classmethod
    def _is_safe_widening(cls, old_type: str, new_type: str) -> bool:
        """Check if changing from old_type to new_type is safe widening."""
        old_norm = cls._normalize_type(old_type)
        new_norm = cls._normalize_type(new_type)
        if old_norm == new_norm:
            return True
        allowed = SAFE_TYPE_WIDENING.get(old_norm, set())
        return new_norm in allowed

    def compare_schemas(
        self,
        target_columns: dict[str, str],
        contract_spec: dict[str, Any],
        mode: str = "BACKWARD",
    ) -> SchemaDiffReport:
        """Compare an incoming column dictionary against a contract specification.

        Parameters
        ----------
        target_columns : dict[str, str]
            Map of column_name -> type_string representing incoming dataset schema.
        contract_spec : dict[str, Any]
            Loaded YAML contract dictionary containing 'columns'.
        mode : str
            Compatibility mode: 'BACKWARD', 'FORWARD', or 'FULL'.
        """
        mode = mode.upper()
        if mode not in ("BACKWARD", "FORWARD", "FULL"):
            raise ValueError(
                f"Unsupported compatibility mode '{mode}'. Choose BACKWARD, FORWARD, or FULL."
            )

        breaking: list[str] = []
        safe: list[str] = []
        type_changes: list[str] = []

        contract_cols: dict[str, dict[str, Any]] = {
            col["name"]: col for col in contract_spec.get("columns", [])
        }

        # 1. Added columns (present in target, not in contract)
        added = [col for col in target_columns if col not in contract_cols]
        for col in added:
            if mode in ("FORWARD", "FULL"):
                # Forward compatibility fails if new fields cannot be processed by old readers
                breaking.append(f"Added column '{col}' violates {mode} compatibility")
            else:
                safe.append(f"Added optional column '{col}'")

        # 2. Removed columns (present in contract, missing in target)
        removed = [col for col in contract_cols if col not in target_columns]
        for col in removed:
            is_required = contract_cols[col].get("required", False)
            if is_required:
                breaking.append(f"Removed required contract column '{col}'")
            else:
                if mode in ("BACKWARD", "FULL"):
                    safe.append(f"Removed optional contract column '{col}'")
                else:
                    breaking.append(f"Removed column '{col}' violates FORWARD compatibility")

        # 3. Type changes on existing columns
        for col, target_type in target_columns.items():
            if col in contract_cols:
                raw_type = contract_cols[col].get("type")
                if raw_type:
                    contract_type = str(raw_type)
                    norm_contract = self._normalize_type(contract_type)
                    norm_target = self._normalize_type(target_type)

                    if norm_contract != norm_target:
                        change_desc = (
                            f"Column '{col}' type changed from '{contract_type}' to '{target_type}'"
                        )
                        type_changes.append(change_desc)

                        if self._is_safe_widening(contract_type, target_type):
                            safe.append(f"Safe type widening: {change_desc}")
                        else:
                            breaking.append(f"Incompatible type change: {change_desc}")

        is_compatible = len(breaking) == 0
        return SchemaDiffReport(
            is_compatible=is_compatible,
            compatibility_mode=mode,
            breaking_changes=breaking,
            safe_evolutions=safe,
            added_columns=added,
            removed_columns=removed,
            type_changes=type_changes,
        )

    def validate_dataframe(
        self,
        df: pl.DataFrame,
        contract_path: Path | str,
        mode: str = "BACKWARD",
    ) -> SchemaDiffReport:
        """Validate a Polars DataFrame schema against a YAML contract file."""
        contract_file = Path(contract_path)
        if not contract_file.is_file():
            raise FileNotFoundError(f"Contract file not found: {contract_path}")

        with contract_file.open("r", encoding="utf-8") as f:
            spec = yaml.safe_load(f)

        target_cols = {col: str(dtype) for col, dtype in df.schema.items()}
        return self.compare_schemas(target_cols, spec, mode=mode)
