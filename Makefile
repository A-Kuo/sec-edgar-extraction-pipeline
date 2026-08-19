# Local commands mirroring CI.
#
# Every target here runs the same command the corresponding CI job runs, so a
# green `make ci` locally means a green pipeline. Where they drift, CI is the
# source of truth and this file is the bug.

.DEFAULT_GOAL := help
.PHONY: help install install-dev lint format typecheck test test-fast coverage \
        dag-check migrate migrate-down audit ci \
        train evaluate model-ci \
        up down logs docker-build docker-run clean

PYTHON ?= python
PYTEST_ARGS ?=
export MOCK_EDGAR ?= true

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

benchmark: ## Run full pipeline end-to-end with timing (mock mode)
	MOCK_EDGAR=true $(PYTHON) -m scripts.benchmark_pipeline

# --- setup ------------------------------------------------------------------

install: ## Install runtime dependencies
	$(PYTHON) -m pip install -r requirements.txt

install-dev: ## Install runtime + development dependencies
	$(PYTHON) -m pip install -r requirements-dev.txt
	pre-commit install

# --- quality ----------------------------------------------------------------

lint: ## Run ruff lint and format checks
	ruff check .
	ruff format --check .

format: ## Auto-fix lint errors and format
	ruff check --fix .
	ruff format .

typecheck: ## Run mypy
	mypy src api scripts

test: ## Run the full test suite with coverage
	pytest tests/ -v --cov=src --cov=api --cov=scripts \
		--cov-report=term-missing --cov-fail-under=75 $(PYTEST_ARGS)

test-fast: ## Run tests without coverage
	pytest tests/ -q $(PYTEST_ARGS)

coverage: ## Write an HTML coverage report to htmlcov/
	pytest tests/ --cov=src --cov=api --cov=scripts --cov-report=html
	@echo "Report: htmlcov/index.html"

dag-check: ## Import the DAG against the installed Airflow (not the mocks)
	pytest tests/test_dag_import.py -v

audit: ## Check dependencies for known vulnerabilities
	pip-audit -r requirements.txt --desc

ci: lint typecheck test dag-check ## Everything CI runs, in CI's order

# --- database ---------------------------------------------------------------

migrate: ## Apply migrations to DATABASE_URL
	alembic upgrade head

migrate-down: ## Roll all migrations back
	alembic downgrade base

# --- machine learning -------------------------------------------------------

train: ## Train and promote a model on synthetic data
	$(PYTHON) -m scripts.train_model --synthetic --promote \
		--metrics-out artifacts/metrics.json

evaluate: ## Gate the latest metrics against floors and the baseline
	$(PYTHON) -m scripts.evaluate_model --metrics artifacts/metrics.json

model-ci: ## Train, gate, and verify — what the Model CI workflow runs
	$(PYTHON) -m scripts.train_model --synthetic --no-register \
		--metrics-out artifacts/metrics.json
	$(PYTHON) -m scripts.evaluate_model --metrics artifacts/metrics.json

# --- local services ---------------------------------------------------------

up: ## Start PostgreSQL and Redis
	docker compose up -d

down: ## Stop PostgreSQL and Redis
	docker compose down

logs: ## Tail service logs
	docker compose logs -f

api: ## Serve the FastAPI app locally on port 8000
	uvicorn api.main:app --reload --port 8000

docker-build: ## Build the application image
	docker build -t sec-edgar-pipeline:local .

docker-run: ## Run the built image on port 8000
	docker run --rm -p 8000:8000 \
		-e DATABASE_URL="$${DATABASE_URL}" \
		-e REDIS_URL="$${REDIS_URL}" \
		-v "$$(pwd)/models:/app/models:ro" \
		sec-edgar-pipeline:local

# --- housekeeping -----------------------------------------------------------

clean: ## Remove caches and build artifacts
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov \
		.coverage coverage.xml artifacts *.egg-info
.PHONY: install demo test test-cov api infra-up infra-down migrate backfill

install:
	pip install -r requirements.txt

## One-command demo: runs the full DAG task chain against MOCK_EDGAR
## fixture data and prints what each stage produced. No Docker, no
## Postgres/Redis, no live network, done in well under a minute.
demo:
	python scripts/demo.py

test:
	pytest tests/ -v

test-cov:
	pytest --cov=src --cov=api tests/

api:
	uvicorn api.main:app --reload --port 8000

infra-up:
	docker-compose up -d

infra-down:
	docker-compose down

migrate:
	alembic upgrade head

backfill:
	python scripts/backfill.py --cik $(CIK) --start-date $(START) --end-date $(END)
