"""Unit test ensuring synchronization between pyproject.toml, uv.lock, and requirements.txt."""

import re
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent


def test_dependency_sync_pyproject_requirements_lock() -> None:
    """Assert all runtime dependencies in pyproject.toml exist in requirements.txt and uv.lock."""
    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    requirements_path = PROJECT_ROOT / "requirements.txt"
    uv_lock_path = PROJECT_ROOT / "uv.lock"

    assert pyproject_path.is_file(), "pyproject.toml is missing"
    assert requirements_path.is_file(), "requirements.txt is missing"
    assert uv_lock_path.is_file(), "uv.lock is missing"

    with open(pyproject_path, "rb") as f:
        pyproject_data = tomllib.load(f)

    runtime_deps = pyproject_data.get("project", {}).get("dependencies", [])
    assert len(runtime_deps) > 0, "No runtime dependencies found in pyproject.toml"

    requirements_content = requirements_path.read_text(encoding="utf-8").lower()
    uv_lock_content = uv_lock_path.read_text(encoding="utf-8").lower()

    for dep in runtime_deps:
        # Extract base package name (e.g. 'psycopg2-binary' from 'psycopg2-binary>=2.9.9,<3.0.0')
        pkg_name = re.split(r"[><=~]", dep)[0].strip().lower()
        assert pkg_name in requirements_content, (
            f"Declared runtime dependency '{pkg_name}' not found in requirements.txt"
        )
        assert pkg_name in uv_lock_content, (
            f"Declared runtime dependency '{pkg_name}' not found in uv.lock"
        )


def test_dependency_bounded_versions() -> None:
    """Verify all runtime dependencies have explicit lower and upper bounds."""
    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject_data = tomllib.load(f)

    runtime_deps = pyproject_data.get("project", {}).get("dependencies", [])
    for dep in runtime_deps:
        assert ">=" in dep or "==" in dep, f"Dependency missing lower bound: {dep}"
        assert "<" in dep or "==" in dep, f"Dependency missing upper bound: {dep}"
