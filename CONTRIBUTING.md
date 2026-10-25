# Contributing to QuantaFlow

Thank you for your interest in contributing to **QuantaFlow**! We welcome bug fixes, performance improvements, and documentation enhancements.

---

## 1. Development Environment Setup

### Prerequisites
- Python 3.11+
- Git
- Docker & Docker Compose (optional, for local PostgreSQL/MinIO/Airflow)

### Quick Setup

```bash
# Clone the repository
git clone https://github.com/Ravikishore710/QuantaFlow.git
cd QuantaFlow

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# Linux / macOS:
source .venv/bin/activate

# Install dependencies in editable mode
pip install --upgrade pip
pip install -e ".[dev,airflow]"

# Set up pre-commit hooks
pre-commit install
```

---

## 2. Quality & Verification Commands

Before opening a pull request, ensure all linters, type checks, and tests pass:

```bash
# Run Ruff linter and formatter checks
ruff check src scripts tests

# Run static type checking with Mypy
mypy src scripts

# Run full test suite with >=85% coverage floor
pytest tests/unit tests/contracts tests/integration tests/e2e --cov=src --cov-fail-under=85

# Run dependency vulnerability audit
pip-audit --desc on
```

Or run via the Makefile:
```bash
make lint
make typecheck
make test
make security
```

---

## 3. Commit Message Conventions

We enforce [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):
- `feat:` A new pipeline feature or capability.
- `fix:` A bug fix or pipeline anomaly correction.
- `refactor:` Code reorganization that does not alter business logic.
- `test:` Adding or updating tests.
- `ci:` Updates to CI/CD workflows, Dockerfile, or Makefile.
- `docs:` Documentation or comment improvements.
- `chore:` Dependency bumps or maintenance tasks.

Every code change in `src/` or `scripts/` MUST be committed alongside corresponding tests in `tests/`.

---

## 4. Pull Request Process

1. Fork the repository and create your branch from `main`:
   ```bash
   git checkout -b feat/your-feature-name
   ```
2. Keep commits atomic, well-described, and paired with automated tests.
3. Ensure test coverage remains $\ge 85\%$.
4. Push your branch and open a Pull Request targeting `main`.
5. Ensure all GitHub Actions CI checks pass green.
