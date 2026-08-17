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
