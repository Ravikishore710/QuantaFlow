.PHONY: install lint typecheck test security run backfill benchmark docker-up clean help

help:
	@echo "QuantaFlow Development Workflow Commands:"
	@echo "  make install     - Install dependencies in editable mode"
	@echo "  make lint        - Run ruff linter and style checks"
	@echo "  make typecheck   - Run static type checking with mypy"
	@echo "  make test        - Run test suite with >=85% coverage floor"
	@echo "  make security    - Run dependency vulnerability audit"
	@echo "  make run         - Execute standard pipeline run for 2024-01"
	@echo "  make backfill    - Run full 6-month historical backfill"
	@echo "  make benchmark   - Run end-to-end performance benchmarks"
	@echo "  make docker-up   - Start local MinIO and PostgreSQL services"
	@echo "  make clean       - Remove cached files and build artifacts"

install:
	pip install --upgrade pip
	pip install -e ".[dev,airflow]"

lint:
	ruff check src scripts tests

typecheck:
	mypy src scripts

test:
	pytest tests/unit tests/contracts tests/integration tests/e2e --cov=src --cov-fail-under=85

security:
	pip install pip-audit
	pip-audit --desc on

run:
	python scripts/run_pipeline.py --period 2024-01

backfill:
	python scripts/backfill.py

benchmark:
	python scripts/benchmark.py

docker-up:
	docker compose up -d

clean:
	rm -rf .pytest_cache .coverage htmlcov .mypy_cache .ruff_cache build dist *.egg-info
