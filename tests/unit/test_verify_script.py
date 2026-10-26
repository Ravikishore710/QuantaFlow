"""Unit test verifying the structure and commands in scripts/verify_fresh_clone.sh."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "verify_fresh_clone.sh"


def test_verify_fresh_clone_script_exists() -> None:
    """Verify that scripts/verify_fresh_clone.sh exists and is non-empty."""
    assert SCRIPT_PATH.is_file(), "scripts/verify_fresh_clone.sh does not exist"
    assert SCRIPT_PATH.stat().st_size > 0, "scripts/verify_fresh_clone.sh is empty"


def test_verify_fresh_clone_script_content() -> None:
    """Verify bash script contains shebang, strict mode, and all validation stages."""
    content = SCRIPT_PATH.read_text(encoding="utf-8")
    assert content.startswith("#!/usr/bin/env bash") or "#!/bin/bash" in content
    assert "set -euo pipefail" in content
    assert "pip install" in content
    assert "ruff check" in content
    assert "mypy" in content
    assert "pytest" in content
    assert "scripts/health_check.py" in content
    assert "exit 0" in content
