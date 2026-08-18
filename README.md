# SEC EDGAR Extraction Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF.svg)](.github/workflows/ci.yml)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

> A data engineering pipeline that ingests SEC 10-K/10-Q filings, extracts structured financial facts from XBRL, scores them for extraction anomalies, validates data quality, and serves the results through a caching API — with CI/CD gating every change, including the model.

## Problem

The SEC publishes every 10-K and 10-Q as an iXBRL document — HTML with financial
facts machine-tagged inline. In principle that makes the numbers trivially
extractable. In practice, five things make a naive parser silently wrong:

**1. Scale attributes.** A revenue figure rendered as `391,035` may carry
`scale="6"`, meaning the real value is 391,035,000,000. Read the text and ignore
the attribute and you are off by six orders of magnitude — and the result still
looks like a plausible number.

**2. Instant versus duration periods.** Total assets is a balance at a point in
time; revenue is a flow across a range. Both arrive as the same tag shape,
distinguished only by whether the referenced context carries an `instant` or a
`startDate`/`endDate` pair. Conflate them and you attribute a year's revenue to
a single day.

**3. Segment disaggregation.** The same concept is frequently tagged twice — once
consolidated, once per reporting segment. Extraction that ignores the segment
axis sums them and reports double the company's actual revenue.

**4. Amendments supersede silently.** A 10-K/A replaces the original 10-K, often
months later, sometimes restating figures. A pipeline without an amendment chain
either double-counts both filings or keeps serving the superseded number
indefinitely.

**5. Rate limits are enforced.** SEC EDGAR caps clients at 10 requests per second
and requires a declared User-Agent. Exceed it and you are blocked — mid-backfill,
with partial state already written.

Underneath all five is one property that shapes the whole design: **a wrong
number that looks plausible costs far more than a missing one.** A dropped fact
is visible on inspection. A revenue figure off by a factor of a thousand flows
into a model, a dashboard, and a decision before anyone checks it. So this
pipeline is built to fail loudly, prove where every number came from, and make
tampering detectable after the fact.

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
│  Airflow DAG (9 tasks)                      │
├────────────────────────────────────────────┤
│ 1. fetch_new_filings      (EdgarClient)     │
│ 2. download_raw_documents                   │
│ 3. parse_xbrl_facts        (XBRLParser)     │
│ 4. validate_quality_gates  (PSI + complete) │
│ 5. score_anomalies         (hybrid model)   │
│ 6. load_to_warehouse       (PostgreSQL)     │
│ 7. update_audit_trail      (append-only)    │
│ 8. collect_run_metrics     (run_metadata)   │
│ 9. send_alerts_on_failure  (Slack/SMTP)     │
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

The diagram above is a summary; below is the actual DAG, rendered by Airflow (`airflow dags show edgar_pipeline`) rather than hand-drawn — the two failure/alerting edges into `send_alerts_on_failure` are structural, not decorative.

![The DAG rendered directly from dags/edgar_pipeline.py by Airflow](docs/images/dag_graph.png)

*Captured before `collect_run_metrics` was added, so it shows 8 of the current 9 tasks; the metrics task chains off `update_audit_trail`.*

## By the Numbers

Everything below is measured, not asserted — reproduce any of it with `make ci` or the command noted.

| Metric | Value |
|---|---|
| Tests | 501 passing (`make test`) |
| Test coverage | 80% across `src`/`api`/`scripts`, gated at ≥75% in CI (`make coverage`) |
| Database tables | 7, across 3 Alembic migrations |
| API endpoints | 8 (`api/main.py`) |
| DAG tasks | 9, one Airflow DAG (`dags/edgar_pipeline.py`) |
| CI/CD workflows | 3 — [`ci.yml`](.github/workflows/ci.yml) (lint, test, migrations, live-Postgres trigger check), [`ml.yml`](.github/workflows/ml.yml) (train + gate the anomaly model), [`cd.yml`](.github/workflows/cd.yml) (tagged image build + publish) |

| Test module | Focus | Count |
|---|---|---|
| `test_ml_model.py` | Hybrid anomaly model (IsolationForest + rules), scoring, serialization | 53 |
| `test_ml_features.py` | Feature extraction and engineering for the anomaly model | 51 |
| `test_api.py` | Endpoints, caching behavior, schemas, per-accession audit history | 50 |
| `test_audit.py` | Hash-chain construction, verification, tamper detection | 40 |
| `test_ml_registry.py` | Model registration, hash verification, promotion | 37 |
| `test_dag.py` | DAG structure, task wiring, extraction-audit wiring (mocked Airflow) | 36 |
| `test_parser.py` | XBRL extraction — units, periods, amendments | 34 |
| `test_upsert.py` | Idempotent UPSERTs, incl. mid-batch worker-restart replay | 32 |
| `test_quality.py` | Completeness thresholds, PSI edge cases | 28 |
| `test_ml_monitoring.py` | PSI drift detection, on facts and on the model | 27 |
| `test_client.py` | Rate limiting, full-jitter retry/backoff | 23 |
| `test_schema.py` | ORM models against real SQLite | 22 |
| `test_scripts_ml.py` | `train_model.py` / `evaluate_model.py` CLIs | 21 |
| `test_alerts.py` | Slack/SMTP alerting | 14 |
| `test_verify_audit_chain.py` | `verify_audit_chain.py` CLI — valid/broken/empty chains | 12 |
| `test_metrics_collection.py` | Per-run metrics aggregation and the append-only metrics log | 9 |
| `test_benchmark_pipeline.py` | Benchmark runner — mock-mode end-to-end, timing instrumentation | 9 |
| `test_dag_import.py` | DAG imports against the **real** installed Airflow | 3 |
| | **Total** | **501** |

## Evidence

The Swagger UI FastAPI generates from the actual route definitions in `api/main.py` — nothing hand-maintained to go stale:

![Swagger UI listing all 8 endpoints, grouped by tag](docs/images/swagger_overview.png)

And a real request against `GET /audit/{accession}`, against a seeded filing — the `row_hash`/`prev_row_hash` chain visible in the response is exactly what `scripts/verify_audit_chain.py` recomputes and checks:

![A live Try-it-out response from GET /audit/{accession}, showing chained row_hash/prev_row_hash values](docs/images/audit_endpoint_response.png)

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── src/
│   ├── schema.py           # SQLAlchemy ORM models (7 tables)
│   ├── edgar_client.py     # EDGAR API client (rate-limited, retry with backoff)
│   ├── xbrl_parser.py      # XBRL HTML -> financial fact extraction
│   ├── quality.py          # Completeness checks + PSI drift detection
│   ├── cache.py            # Redis caching (CIK, filing index, facts)
│   ├── alerts.py           # Slack/SMTP alerting on pipeline failure
│   ├── metrics.py          # Per-run metrics aggregation -> metrics/run_metadata.json
│   └── ml/                  # Anomaly detection (features, rules, model, registry, monitoring)
├── api/
│   └── main.py              # FastAPI serving layer (8 endpoints)
├── dags/
│   └── edgar_pipeline.py    # Airflow DAG (9-task pipeline)
├── scripts/
│   ├── backfill.py          # CLI: historical ingestion by CIK + date range
│   ├── validate.py          # CLI: run quality checks for a given run_id
│   ├── train_model.py       # CLI: train + register (+ optionally promote) a model
│   ├── evaluate_model.py    # CLI: CI gate — floors + regression vs. promoted model
│   ├── verify_audit_chain.py # CLI: recompute and verify the extraction hash chain
│   └── benchmark_pipeline.py # CLI: end-to-end timing in mock mode (`make benchmark`)
├── migrations/               # Alembic migration environment + versions
├── metrics/                  # Append-only per-run metrics log
├── docs/
│   ├── API_CONTRACTS.md      # Endpoint reference and interface conventions
│   ├── DECISION_LOG.md       # Defects found and how they were corrected
│   ├── AUDIT_TRAIL_PLAN.md   # Design record for the hash-chained audit trail
│   └── MICROSERVICE_ALTERNATIVE.md # Why this is a monolith, and the split alternative
├── tests/                    # pytest suite — 501 tests, 80% coverage on gated modules
├── .github/workflows/        # ci.yml, ml.yml, cd.yml
├── Dockerfile                 # Multi-stage build, non-root runtime
├── docker-compose.yml         # PostgreSQL 16 + Redis 7
├── Makefile                   # Local commands mirroring CI
├── pyproject.toml             # pytest / coverage / ruff / mypy config
├── requirements.txt / requirements-dev.txt
├── alembic.ini
├── AGENTS.md                  # Architecture spec + build notes for contributors/agents
├── MODEL_CARD.md              # Anomaly model: intended use, limits, evaluation method
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
pytest tests/ -v                        # full suite (501 tests) — see "By the Numbers" for the per-module breakdown
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
- **DB-enforced, hash-chained audit trail.** `pipeline_audit` (per DAG stage) and `extraction_audit` (per accession, per extraction attempt) are append-only — not just by convention, but enforced by a PostgreSQL trigger that rejects any `UPDATE`/`DELETE` outright. Each `extraction_audit` row's hash covers its own fields plus the previous row's hash, so tampering any historical row — even by an actor with direct DB access bypassing the trigger — is detectable by recomputing the chain (`scripts/verify_audit_chain.py`, or `GET /audit/{accession}` for a single accession's history).
- **Cache-first API, with the ML endpoints exempted.** Filing/fact reads check Redis first and fall back to PostgreSQL. `/anomalies` and `/model/current` skip the cache deliberately — scores change whenever a new model is promoted, and a stale score is worse than a slow one.
- **Rate-limited ingestion.** A token-bucket limiter keeps requests to SEC EDGAR at or below 10 req/s, per their access guidelines.
- **Drift-aware quality gates, on facts and on the model.** PSI on extracted fact distributions flags data quality regressions before they reach the warehouse; the same PSI machinery, reused rather than reimplemented, monitors the anomaly model's own feature and prediction distributions for drift.
- **A real DAG-import test, not just a mocked one.** `tests/test_dag.py` mocks Airflow away for fast structural tests, which once let an Airflow-3-incompatible DAG pass 131/131 tests while failing to import in production. `tests/test_dag_import.py` imports the module in a subprocess against whatever Airflow is actually installed, specifically to close that gap.

## Where This Fits

SEC filings contain two kinds of data, and they need two different extraction
strategies. This repository handles the first; a sibling repository handles the
second.

**Tagged facts — this repository.** Roughly the core financial statements:
figures the filer machine-tagged in iXBRL. Because the markup declares the
concept, unit, scale, and period, extraction is *deterministic* — `lxml` reads
the tags, and every value traces to a specific element in a specific document.
There is no model, so there is no hallucination risk, and the hash-chained audit
trail makes any post-hoc alteration detectable.

**Untagged prose — [Fine-Tuned-SEC-Filing-Extraction-Pipeline](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline).**
Everything the tagging does not reach: figures quoted in MD&A, footnote detail,
non-GAAP reconciliations, narrative tables. No markup declares what those numbers
mean, so deterministic parsing cannot recover them. That repository fine-tunes
Llama 3.1 8B with QLoRA to extract them, and evaluates the result the way a
probabilistic extractor has to be evaluated.

The split is deliberate rather than accidental. The two halves have different
correctness criteria (verifiable against source markup vs. measured against a
held-out set), different infrastructure (scheduled CPU batch jobs vs. GPU
inference serving), and different failure modes. Merging them would put a
multi-gigabyte CUDA dependency stack inside an Airflow worker image and force one
CI pipeline to gate two unrelated notions of "correct."

### Related repositories

| Repository | Role |
|---|---|
| [Fine-Tuned-SEC-Filing-Extraction-Pipeline](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline) | QLoRA fine-tuned Llama 3.1 8B — extracts from untagged filing prose |
| [Transformer-Aspect-Based-Sentiment-Analysis](https://github.com/A-Kuo/Transformer-Aspect-Based-Sentiment-Analysis) | Aspect-level sentiment over MD&A and risk-factor text |
| [Financial-Economic-Ticker-Analyzer-Agent](https://github.com/A-Kuo/Financial-Economic-Ticker-Analyzer-Agent) | Market-intelligence enrichment keyed on extracted ticker |
| [Agentic-Visualization-Framework](https://github.com/A-Kuo/Agentic-Visualization-Framework) | Dashboard generation over the structured output |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow, and [AGENTS.md](AGENTS.md) for the architecture spec, schema definitions, and implementation notes. This project follows a [Code of Conduct](CODE_OF_CONDUCT.md).

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and verify: `make ci` (lint, typecheck, tests, real DAG import — the same checks CI runs)
3. Commit with a descriptive message and open a pull request

Optionally, `pre-commit install` to catch lint/format/type issues before they reach CI.

## Citation

```bibtex
@software{sec_edgar_extraction_pipeline_2026,
  author = {Kuo, Austin},
  title = {SEC EDGAR Extraction Pipeline},
  url = {https://github.com/A-Kuo/sec-edgar-extraction-pipeline},
  year = {2026}
}
```

See [CITATION.cff](CITATION.cff).

## License

MIT License — see [LICENSE](LICENSE).

## Contact

Issues and questions: [GitHub Issues](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/issues)

---

**Status:** Core pipeline, ML anomaly-detection layer, model registry, and CI/CD (lint/typecheck/test, model train+gate, Docker build+push to GHCR) complete — 501 tests, 80% coverage on CI-gated modules | **Last updated:** August 2026
