# SEC EDGAR Extraction Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF.svg)](.github/workflows/ci.yml)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

> A data engineering pipeline that ingests SEC 10-K/10-Q filings, extracts structured financial facts from XBRL, scores them for extraction anomalies, validates data quality, and serves the results through a caching API — with CI/CD gating every change, including the model.

## Problem

SEC filings are published as unstructured HTML/XBRL documents. Pulling a single metric — revenue, net income, total assets — for a set of companies over time means manually locating filings, parsing inconsistent markup, and reconciling amendments. This doesn't scale past a handful of one-off lookups. And a parser that fails silently on a scale error or a dropped fact is worse than one that fails loudly — a wrong number that looks plausible costs far more to catch after the fact than a missing one.

## Solution

This pipeline automates the full path from raw filing to queryable, versioned, quality-checked financial data:

1. **Ingest** — an SEC EDGAR API client (rate-limited, retrying) pulls filing metadata and raw documents by CIK/ticker.
2. **Parse** — an XBRL parser extracts a fixed set of financial facts (revenue, net income, assets, liabilities, EPS, etc.), normalizing units and period types. Deterministic, no LLM in the extraction path — a reported number is always traceable to its source document.
3. **Validate** — a quality layer checks field completeness against a threshold and flags statistical drift (PSI) in extracted values before anything is trusted downstream.
4. **Score** — a hybrid anomaly detector (IsolationForest + deterministic plausibility rules) flags filings whose *extracted* facts look internally inconsistent — a scale error, a dropped required fact, an EPS that doesn't reconcile with net income and shares — producing a ranked review queue instead of leaving that discovery to a manual audit. The model never touches a reported value; a flag routes a filing to a human.
5. **Store** — validated, scored facts land in PostgreSQL with an append-only audit trail and amendment-aware version history.
6. **Serve** — a FastAPI layer exposes filings, time-series facts, and anomaly scores, backed by a Redis cache.

The pipeline is orchestrated end-to-end by an Airflow DAG. CI trains and gates the anomaly model on every relevant change, and a tagged release builds and publishes a container image.

## Architecture

```
SEC EDGAR API
     │
     ▼
┌────────────────────────────────────────────┐
│  Airflow DAG (8 tasks)                      │
├────────────────────────────────────────────┤
│ 1. fetch_new_filings      (EdgarClient)     │
│ 2. download_raw_documents                   │
│ 3. parse_xbrl_facts        (XBRLParser)     │
│ 4. validate_quality_gates  (PSI + complete) │
│ 5. score_anomalies         (hybrid model)   │
│ 6. load_to_warehouse       (PostgreSQL)     │
│ 7. update_audit_trail      (append-only)    │
│ 8. send_alerts_on_failure  (Slack/SMTP)     │
└────────────────────────────────────────────┘
     │
     ▼
┌────────────────────────┐    ┌────────────────────────┐
│  PostgreSQL             │    │  Redis Cache            │
│  filings_raw, facts,    │◄───┤  CIK (1h), filing index │
│  versions, audit trail, │    │  (24h), facts (7d)      │
│  model_runs, anomalies  │    │                          │
└────────────────────────┘    └────────────────────────┘
     │
     ▼
┌────────────────────────────────────────────┐
│  FastAPI serving layer                      │
│  health / filings / filing / facts /        │
│  anomalies / model / trigger                │
└────────────────────────────────────────────┘

┌────────────────────────────────────────────┐
│  Model registry (models/)                   │
│  content-addressed, verified on load,       │
│  promoted via CI (.github/workflows/ml.yml) │
└────────────────────────────────────────────┘
```

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── src/
│   ├── schema.py           # SQLAlchemy ORM models (6 tables)
│   ├── edgar_client.py     # EDGAR API client (rate-limited, retry with backoff)
│   ├── xbrl_parser.py      # XBRL HTML -> financial fact extraction
│   ├── quality.py          # Completeness checks + PSI drift detection
│   ├── cache.py            # Redis caching (CIK, filing index, facts)
│   ├── alerts.py           # Slack/SMTP alerting on pipeline failure
│   └── ml/                  # Anomaly detection (features, rules, model, registry, monitoring)
├── api/
│   └── main.py              # FastAPI serving layer (7 endpoints)
├── dags/
│   └── edgar_pipeline.py    # Airflow DAG (8-task pipeline)
├── scripts/
│   ├── backfill.py          # CLI: historical ingestion by CIK + date range
│   ├── validate.py          # CLI: run quality checks for a given run_id
│   ├── train_model.py       # CLI: train + register (+ optionally promote) a model
│   └── evaluate_model.py    # CLI: CI gate — floors + regression vs. promoted model
├── migrations/               # Alembic migration environment + versions
├── tests/                    # pytest suite — 363 tests, 77% coverage on gated modules
├── .github/workflows/        # ci.yml, ml.yml, cd.yml
├── Dockerfile                 # Multi-stage build, non-root runtime
├── docker-compose.yml         # PostgreSQL 16 + Redis 7
├── Makefile                   # Local commands mirroring CI
├── pyproject.toml             # pytest / coverage / ruff / mypy config
├── requirements.txt / requirements-dev.txt
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

cp .env.example .env   # then edit as needed
pip install -r requirements.txt -r requirements-dev.txt

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

# Train + promote an anomaly model (synthetic data — no live filings needed)
python -m scripts.train_model --synthetic --promote

# Run the test suite
pytest tests/ -v

# Start the API
uvicorn api.main:app --reload --port 8000
# Interactive docs at http://localhost:8000/docs
```

Or with `make` (mirrors CI exactly — see `make help`):

```bash
make install-dev
make up && make migrate
make train
make ci
```

## Usage

### API

Filing/fact reads check Redis before hitting PostgreSQL; anomaly and model endpoints deliberately bypass the cache.

```bash
curl http://localhost:8000/health

curl http://localhost:8000/filings/AAPL?limit=10&offset=0

curl http://localhost:8000/filing/0000320193-23-000077

curl http://localhost:8000/facts/AAPL/Revenues

curl http://localhost:8000/anomalies/AAPL?only_anomalies=true

curl http://localhost:8000/model/current

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

### Train and evaluate the anomaly model

```bash
# Train on synthetic data, register (don't promote), write metrics for the gate
python -m scripts.train_model --synthetic --metrics-out artifacts/metrics.json

# Gate the candidate against absolute floors and the promoted baseline
python -m scripts.evaluate_model --metrics artifacts/metrics.json

# Promote once the gate passes
python -m scripts.train_model --synthetic --promote
```

`scripts/evaluate_model.py` exits 0 (pass), 1 (failed a floor or regressed), or 2 (could not evaluate — missing file, empty registry). This is what `.github/workflows/ml.yml` runs on every PR touching `src/ml/**`.

### Airflow DAG

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow dags unpause edgar_pipeline
airflow dags test edgar_pipeline 2024-01-15
```

Every task writes a start/end row to `pipeline_audit`, which is append-only — no `UPDATE`/`DELETE` is ever issued against it. `score_anomalies` is the exception to the otherwise-strict pipeline: a missing or unverifiable model degrades to "no scores" rather than failing the run.

### Docker

```bash
make docker-build
make docker-run   # requires DATABASE_URL / REDIS_URL and a mounted models/ volume
```

Tagged releases (`vX.Y.Z`) are built and pushed to GHCR automatically by `.github/workflows/cd.yml`, with an SBOM and a Trivy vulnerability scan attached to the run.

## Testing

```bash
pytest tests/ -v                        # full suite (363 tests)
pytest tests/test_api.py -v             # endpoints + caching behavior
pytest tests/test_client.py -v          # rate limiting, retry/backoff
pytest tests/test_parser.py -v          # XBRL extraction, units, periods
pytest tests/test_quality.py -v         # completeness + PSI edge cases
pytest tests/test_dag.py -v             # DAG structure and task wiring (mocked Airflow)
pytest tests/test_dag_import.py -v      # DAG imports against the REAL installed Airflow
pytest tests/test_schema.py -v          # ORM models against real SQLite
pytest tests/test_ml_*.py -v            # features, model, registry, monitoring
pytest tests/test_scripts_ml.py -v      # train_model.py / evaluate_model.py CLIs
pytest --cov=src --cov=api --cov=scripts tests/   # with coverage (gated at 75% in CI)
```

`tests/test_dag.py` mocks Airflow away for fast structural tests; `tests/test_dag_import.py` exists specifically because that mocking once let an Airflow-3-incompatible DAG stay green for 131/131 tests — see that file's docstring.

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
| `MODEL_REGISTRY_ROOT` | `models` | Directory the anomaly-detection model registry reads/writes |

See [`.env.example`](.env.example) for the full list with inline comments.

## Key Design Decisions

The bullets below state conclusions. [`docs/DECISION_LOG.md`](docs/DECISION_LOG.md) shows the work behind them — the specific defects each decision corrects, with commit hashes and measured before/after — plus the architecture choices made deliberately from day one rather than arrived at by correcting a mistake.

- **No LLM in the extraction path.** XBRL parsing is deterministic (`lxml`), so extraction is reproducible, auditable, and carries no hallucination risk. The anomaly model is downstream of extraction — it scores facts, it never produces them.
- **Hybrid anomaly detection, not just a model.** An IsolationForest alone measured 0.17 recall on dropped required facts, because that feature has zero variance in training and gets flattened by standardization before any tree sees it. Deterministic plausibility rules (`src/ml/rules.py`) catch the failure modes worth naming explicitly; the forest catches what nobody thought to write a rule for. A filing's score is `max(model_score, rule_score)`, with the specific rule violations attached — a review queue an analyst can act on, not a number they have to trust blindly.
- **A verified, versioned model registry.** Every model version's artifact hash, training-data hash, git commit, and evaluation metrics are recorded at registration; `verify()` re-hashes the artifact before every load, so a model swapped on disk after registration fails to load rather than silently scoring production traffic.
- **CI gates the model, not just the code.** `.github/workflows/ml.yml` trains a candidate, checks it against absolute metric floors and the currently-promoted model, and verifies training is reproducible (same flags -> same artifact hash) before anything merges.
- **Append-only audit trail.** `pipeline_audit` is never updated or deleted from — it's a permanent record of every pipeline run.
- **Cache-first API, with the ML endpoints exempted.** Filing/fact reads check Redis first and fall back to PostgreSQL. `/anomalies` and `/model/current` skip the cache deliberately — scores change whenever a new model is promoted, and a stale score is worse than a slow one.
- **Rate-limited ingestion.** A token-bucket limiter keeps requests to SEC EDGAR at or below 10 req/s, per their access guidelines.
- **Drift-aware quality gates, on facts and on the model.** PSI on extracted fact distributions flags data quality regressions before they reach the warehouse; the same PSI machinery, reused rather than reimplemented, monitors the anomaly model's own feature and prediction distributions for drift.
- **A real DAG-import test, not just a mocked one.** `tests/test_dag.py` mocks Airflow away for fast structural tests, which once let an Airflow-3-incompatible DAG pass 131/131 tests while failing to import in production. `tests/test_dag_import.py` imports the module in a subprocess against whatever Airflow is actually installed, specifically to close that gap.

## Contributing

See [AGENTS.md](AGENTS.md) for the full architecture spec, schema definitions, and implementation notes.

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and verify: `make ci` (lint, typecheck, tests, real DAG import — the same checks CI runs)
3. Commit with a descriptive message and open a pull request

Optionally, `pre-commit install` to catch lint/format/type issues before they reach CI.

## License

MIT License — see [LICENSE](LICENSE).

## Contact

Issues and questions: [GitHub Issues](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/issues)

---

**Status:** Core pipeline, ML anomaly-detection layer, model registry, and CI/CD (lint/typecheck/test, model train+gate, Docker build+push to GHCR) complete — 363 tests, 77% coverage on CI-gated modules | **Last updated:** August 2026
