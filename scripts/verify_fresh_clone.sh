#!/usr/bin/env bash
# ==============================================================================
# QuantaFlow Fresh-Clone Verification Script
# Validates environment installation, linting, typing, tests, and health checks
# ==============================================================================
set -euo pipefail

echo "=================================================="
echo "QuantaFlow Fresh-Clone Environment Verification"
echo "=================================================="

# 1. Verify Python version
echo "[1/5] Checking Python interpreter..."
python --version

# 2. Install package in editable mode with dev & airflow extras
echo "[2/5] Installing QuantaFlow package and dependencies..."
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -e ".[dev,airflow]"

# 3. Static code analysis and linting
echo "[3/5] Running Ruff and Mypy checks..."
ruff check src scripts tests evaluation airflow
mypy src scripts

# 4. Execute test suite with coverage enforcement
echo "[4/5] Running full test suite with coverage..."
pytest tests/unit tests/contracts tests/integration tests/e2e --cov=src --cov-fail-under=85

# 5. Pipeline health check CLI probe
echo "[5/5] Executing pipeline health check probe..."
python scripts/health_check.py --mode cli

echo "=================================================="
echo "Fresh-clone environment verification SUCCEEDED!"
echo "=================================================="
exit 0
