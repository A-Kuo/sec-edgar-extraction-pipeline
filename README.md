# SEC EDGAR Extraction Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

> A data engineering pipeline that ingests SEC 10-K/10-Q filings, extracts structured financial facts from XBRL, validates data quality, and serves the results through a caching API.

## Problem

SEC filings are published as unstructured HTML/XBRL documents. Pulling a single metric — revenue, net income, total assets — for a set of companies over time means manually locating filings, parsing inconsistent markup, and reconciling amendments. This doesn't scale past a handful of one-off lookups.

## Solution

This pipeline automates the full path from raw filing to queryable, versioned financial data:

1. **Ingest** — an SEC EDGAR API client (rate-limited, retrying) pulls filing metadata and raw documents by CIK/ticker.
2. **Parse** — an XBRL parser extracts a fixed set of financial facts (revenue, net income, assets, liabilities, EPS, etc.), normalizing units and period types.
3. **Validate** — a quality layer checks field completeness against a threshold and flags statistical drift (PSI) in extracted values before anything is trusted downstream.
4. **Store** — validated facts land in PostgreSQL with an append-only audit trail and amendment-aware version history.
5. **Serve** — a FastAPI layer exposes filings and time-series facts, backed by a Redis cache.

The pipeline is orchestrated end-to-end by an Airflow DAG and runs entirely deterministically — no LLM is involved in extraction.

## Architecture

```
SEC EDGAR API
     │
     ▼
┌───────────────────────────────────────────┐
│  Airflow DAG (7 tasks)                    │
├───────────────────────────────────────────┤
│ 1. fetch_new_filings      (EdgarClient)   │
│ 2. download_raw_documents                 │
│ 3. parse_xbrl_facts        (XBRLParser)   │
│ 4. validate_quality_gates  (PSI + complete)│
│ 5. load_to_warehouse       (PostgreSQL)   │
│ 6. update_audit_trail      (append-only)  │
│ 7. send_alerts_on_failure  (Slack/SMTP)   │
└───────────────────────────────────────────┘
     │
     ▼
┌────────────────────────┐    ┌────────────────────────┐
│  PostgreSQL             │    │  Redis Cache            │
│  filings_raw, facts,    │◄───┤  CIK (1h), filing index │
│  versions, audit trail  │    │  (24h), facts (7d)      │
└────────────────────────┘    └────────────────────────┘
     │
     ▼
┌────────────────────────────────────────┐
│  FastAPI serving layer                  │
│  health / filings / filing / facts /    │
│  trigger                                 │
└────────────────────────────────────────┘
```

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── src/
│   ├── schema.py           # SQLAlchemy ORM models (4 tables)
│   ├── edgar_client.py     # EDGAR API client (rate-limited, retry with backoff)
│   ├── xbrl_parser.py      # XBRL HTML -> financial fact extraction
│   ├── quality.py          # Completeness checks + PSI drift detection
│   ├── cache.py            # Redis caching (CIK, filing index, facts)
│   └── alerts.py           # Slack/SMTP alerting on pipeline failure
├── api/
│   └── main.py              # FastAPI serving layer
├── dags/
│   └── edgar_pipeline.py    # Airflow DAG (7-task pipeline)
├── scripts/
│   ├── backfill.py          # CLI: historical ingestion by CIK + date range
│   └── validate.py          # CLI: run quality checks for a given run_id
├── migrations/               # Alembic migration environment + versions
├── tests/                    # pytest suite (client, parser, quality, DAG, API)
├── docker-compose.yml        # PostgreSQL 16 + Redis 7
├── requirements.txt
├── alembic.ini
├── AGENTS.md                  # Architecture spec + build notes for contributors/agents
└── README.md
```

## Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 16 and Redis 7 (via Docker Compose, or run natively)

### Installation

```bash
git clone https://github.com/A-Kuo/sec-edgar-extraction-pipeline.git
cd sec-edgar-extraction-pipeline

pip install -r requirements.txt

export DATABASE_URL=postgresql://sec_user:sec_pass@localhost/sec_edgar
export REDIS_URL=redis://localhost:6379/0
export SEC_USER_AGENT="your-app-name your.email@example.com"  # required by SEC; omitting it returns 403
export MOCK_EDGAR=true   # skip live API calls during local development/tests
```

### Quick Start

```bash
# Start PostgreSQL + Redis
docker-compose up -d

# Apply schema migrations
alembic upgrade head

# Run the test suite
pytest tests/ -v

# Start the API
uvicorn api.main:app --reload --port 8000
# Interactive docs at http://localhost:8000/docs
```

## Usage

### API

All read endpoints check Redis before hitting PostgreSQL.

```bash
curl http://localhost:8000/health

curl http://localhost:8000/filings/AAPL?limit=10&offset=0

curl http://localhost:8000/filing/0000320193-23-000077

curl http://localhost:8000/facts/AAPL/Revenues

curl -X POST http://localhost:8000/trigger/AAPL
```

### Backfill historical data

```bash
python scripts/backfill.py \
  --cik 0000320193 \
  --start-date 2020-01-01 \
  --end-date 2024-01-01
```

### Run quality checks against a pipeline run

```bash
python scripts/validate.py --run-id backfill-0000320193-2024-01-01
```

### Airflow DAG

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow dags unpause edgar_pipeline
airflow dags test edgar_pipeline 2024-01-15
```

Every task writes a start/end row to `pipeline_audit`, which is append-only — no `UPDATE`/`DELETE` is ever issued against it.

## Testing

```bash
pytest tests/ -v                     # full suite
pytest tests/test_api.py -v          # endpoints + caching behavior
pytest tests/test_client.py -v       # rate limiting, retry/backoff
pytest tests/test_parser.py -v       # XBRL extraction, units, periods
pytest tests/test_quality.py -v      # completeness + PSI edge cases
pytest tests/test_dag.py -v          # DAG structure and task wiring
pytest --cov=src --cov=api tests/    # with coverage
```

Tests run against an in-memory SQLite database and a mocked Redis client, with `MOCK_EDGAR=true` substituting fixture responses for live SEC API calls — no external services are required to run the suite.

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://sec_user:sec_pass@localhost/sec_edgar` | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `SEC_USER_AGENT` | none | Required by SEC EDGAR; requests without it return 403 |
| `MOCK_EDGAR` | `false` | Set `true` for local dev/tests to use fixture data instead of live calls |
| `SLACK_WEBHOOK_URL` | unset | Optional Slack alerting on pipeline failure |
| `SMTP_HOST`, `ALERT_EMAIL_TO` | unset | Optional email alerting on pipeline failure |

## Key Design Decisions

- **No LLM in the extraction path.** XBRL parsing is deterministic (`lxml`), so extraction is reproducible and carries no hallucination risk.
- **Append-only audit trail.** `pipeline_audit` is never updated or deleted from — it's a permanent record of every pipeline run.
- **Cache-first API.** Every read endpoint checks Redis first and falls back to PostgreSQL, with graceful degradation if Redis is unavailable.
- **Rate-limited ingestion.** A token-bucket limiter keeps requests to SEC EDGAR at or below 10 req/s, per their access guidelines.
- **Drift-aware quality gates.** PSI (Population Stability Index) on extracted fact distributions flags data quality regressions before they reach the warehouse, not after.

## Contributing

See [AGENTS.md](AGENTS.md) for the full architecture spec, schema definitions, and implementation notes.

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and verify: `pytest tests/ -v`
3. Commit with a descriptive message and open a pull request

## License

MIT License — see [LICENSE](LICENSE).

## Contact

Issues and questions: [GitHub Issues](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/issues)

---

**Status:** Core pipeline complete (ingestion, parsing, quality, caching, API, DAG, tests) | **Last updated:** August 2026
