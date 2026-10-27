"""AST-based contract test ensuring all environment variables are declared in .env.example.

Prevents configuration drift across source code, scripts, Airflow DAGs, and documentation.
"""

import ast
import re
from pathlib import Path

from src.config.settings import Settings

PROJECT_ROOT = Path(__file__).parent.parent.parent


def get_env_example_keys() -> set[str]:
    """Parse .env.example and return all active environment variable keys."""
    env_example_path = PROJECT_ROOT / ".env.example"
    assert env_example_path.is_file(), ".env.example file does not exist"

    keys: set[str] = set()
    for line in env_example_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key = line.split("=", 1)[0].strip()
            keys.add(key)
    return keys


def is_os_environ_node(node: ast.AST) -> bool:
    """Check if AST node represents os.environ."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def extract_env_vars_from_ast(py_path: Path) -> set[str]:
    """Extract environment variable names accessed in a python file via AST analysis."""
    found: set[str] = set()
    try:
        content = py_path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(py_path))
    except Exception:
        return found

    for node in ast.walk(tree):
        # Match os.getenv("VAR", ...) or os.environ.get("VAR", ...)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            is_getenv = (
                node.func.attr == "getenv"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "os"
            )
            is_environ_get = node.func.attr == "get" and is_os_environ_node(node.func.value)
            if (is_getenv or is_environ_get) and node.args:
                arg0 = node.args[0]
                if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
                    val = arg0.value.strip()
                    if re.match(r"^[A-Z0-9_]+$", val):
                        found.add(val)

        # Match os.environ["VAR"]
        elif (
            isinstance(node, ast.Subscript)
            and is_os_environ_node(node.value)
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            val = node.slice.value.strip()
            if re.match(r"^[A-Z0-9_]+$", val):
                found.add(val)

    return found


def test_env_example_parity() -> None:
    """Assert all environment variables accessed in code are documented in .env.example."""
    example_keys = get_env_example_keys()

    # 1. Check all Pydantic Settings fields
    settings_fields = {
        name.upper()
        for name in Settings.model_fields
    }
    missing_settings = settings_fields - example_keys
    assert not missing_settings, (
        f"Settings fields missing from .env.example: {sorted(list(missing_settings))}"
    )

    # 2. Check AST-scanned os.getenv and os.environ calls
    scanned_vars: set[str] = set()
    for directory in ["src", "scripts", "airflow"]:
        dir_path = PROJECT_ROOT / directory
        if not dir_path.exists():
            continue
        for py_file in dir_path.rglob("*.py"):
            scanned_vars.update(extract_env_vars_from_ast(py_file))

    missing_scanned = scanned_vars - example_keys
    assert not missing_scanned, (
        f"Environment variables referenced in code missing from .env.example: "
        f"{sorted(list(missing_scanned))}"
    )
