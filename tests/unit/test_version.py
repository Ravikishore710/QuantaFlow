"""Unit test verifying project package release version synchronization."""

import tomllib
from pathlib import Path

import src

PROJECT_ROOT = Path(__file__).parent.parent.parent


def test_package_version_sync() -> None:
    """Verify src.__version__ matches pyproject.toml version exactly."""
    assert src.__version__ == "1.4.0"

    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)

    declared_version = data.get("project", {}).get("version")
    assert declared_version == src.__version__
