.PHONY: help install install-dev test lint format type-check pre-commit docker-up docker-down migrate db-reset clean

help:
	@echo "SEC EDGAR Extraction Pipeline — Common Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install production dependencies"
	@echo "  make install-dev      Install dev dependencies (pytest, ruff, mypy, pre-commit)"
	@echo "  make pre-commit       Install pre-commit hooks"
	@echo ""
	@echo "Development:"
	@echo "  make test             Run all tests with coverage"
	@echo "  make test-fast        Run tests without coverage"
	@echo "  make lint             Run ruff linter"
	@echo "  make format           Auto-format code with ruff"
	@echo "  make type-check       Run mypy type checker"
	@echo "  make quality          Run lint + type-check (no fix)"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make docker-up        Start PostgreSQL + Redis"
	@echo "  make docker-down      Stop PostgreSQL + Redis"
	@echo "  make migrate          Run Alembic migrations"
	@echo "  make db-reset         Drop and recreate database"
	@echo ""
	@echo "API:"
	@echo "  make serve            Start FastAPI server (uvicorn)"
	@echo "  make docs             Open API docs at http://localhost:8000/docs"
	@echo ""
	@echo "Data:"
	@echo "  make backfill-test    Backfill sample data (Apple 2020-2024)"
	@echo "  make validate         Validate last backfill run"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean            Remove __pycache__, .pytest_cache, etc."

install:
	pip install -r requirements.txt

install-dev:
	pip install -e ".[dev,migrations]"

pre-commit:
	pre-commit install
	pre-commit run --all-files

test:
	pytest tests/ -v --cov=src --cov=api --cov-report=term-missing --cov-report=html

test-fast:
	pytest tests/ -v

lint:
	ruff check src/ api/ dags/ scripts/ tests/

format:
	ruff format src/ api/ dags/ scripts/ tests/
	ruff check src/ api/ dags/ scripts/ tests/ --fix

type-check:
	mypy src/ api/ dags/ scripts/ --ignore-missing-imports

quality: lint type-check
	@echo "✓ Code quality checks passed"

docker-up:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@docker-compose ps

docker-down:
	docker-compose down

migrate:
	alembic upgrade head

db-reset: docker-down docker-up
	sleep 2
	alembic downgrade base || true
	alembic upgrade head
	@echo "✓ Database reset complete"

serve:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

docs:
	@echo "Opening http://localhost:8000/docs"
	@python -m webbrowser "http://localhost:8000/docs" || echo "Please visit http://localhost:8000/docs manually"

backfill-test: migrate
	python scripts/backfill.py --cik 0000320193 --start-date 2020-01-01 --end-date 2024-01-01

validate:
	python scripts/validate.py --run-id backfill-0000320193-2024-01-01

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage htmlcov/ .mypy_cache/ dist/ build/ *.egg-info/
	@echo "✓ Cleaned up temporary files"
