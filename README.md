# SEC EDGAR Extraction Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

> A data engineering pipeline that ingests SEC 10-K/10-Q filings, extracts structured financial facts from XBRL, validates data quality, serves the results through a caching API, and answers citation-grounded questions over the filing text itself.

## Problem

SEC filings are published as unstructured HTML/XBRL documents. Pulling a single metric — revenue, net income, total assets — for a set of companies over time means manually locating filings, parsing inconsistent markup, and reconciling amendments. This doesn't scale past a handful of one-off lookups.

## Solution

This pipeline automates the full path from raw filing to queryable, versioned financial data:

1. **Ingest** — an SEC EDGAR API client (rate-limited, retrying) pulls filing metadata and raw documents by CIK/ticker.
2. **Parse** — an XBRL parser extracts a fixed set of financial facts (revenue, net income, assets, liabilities, EPS, etc.), normalizing units and period types.
3. **Validate** — a quality layer checks field completeness against a threshold and flags statistical drift (PSI) in extracted values before anything is trusted downstream.
4. **Store** — validated facts land in PostgreSQL with an append-only audit trail and amendment-aware version history.
5. **Serve** — a FastAPI layer exposes filings and time-series facts, backed by a Redis cache.
6. **Research** — a retrieval layer chunks filing text into Item-level sections and answers natural-language questions with citations back to the specific accession and section, refusing to answer when nothing in the indexed filings actually supports it.

The structured-extraction pipeline (steps 1-5) is orchestrated end-to-end by an Airflow DAG and runs entirely deterministically — no LLM is involved. The retrieval layer (step 6) is the one place an LLM can optionally participate, and only to *phrase* an answer already constrained to retrieved filing text — never to originate facts. See [Retrieval &amp; Q&amp;A](#retrieval--qa) below.

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
│   ├── alerts.py           # Slack/SMTP alerting on pipeline failure
│   └── rag/
│       ├── chunker.py       # Filing text -> provenance-tagged chunks
│       ├── retrieval.py     # TF-IDF retrieval over chunks
│       ├── qa.py            # Citation-first answer construction
│       └── evaluation.py    # Retrieval recall / citation validity / refusal-accuracy harness
├── api/
│   └── main.py              # FastAPI serving layer (includes /ask/{ticker})
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

### Retrieval &amp; Q&amp;A

```bash
curl -G http://localhost:8000/ask/AAPL --data-urlencode "q=What supply chain risk does the company describe?"
```

```json
{
  "ticker": "AAPL",
  "question": "What supply chain risk does the company describe?",
  "answer": "Based on 10-K 0000320193-24-000123 (Item 1A. Risk Factors): The Company relies on single-source and limited-source suppliers for some components, which increases the Company's supply chain risk...",
  "grounded": true,
  "citations": [
    {"accession_number": "0000320193-24-000123", "section": "Item 1A. Risk Factors", "snippet": "...", "score": 0.34}
  ]
}
```

If nothing in the ticker's indexed filing text is relevant to the question, the response comes back with `"grounded": false` and an explicit refusal instead of a guess:

```bash
curl -G http://localhost:8000/ask/AAPL --data-urlencode "q=What is the CEO's favorite color?"
# {"grounded": false, "answer": "I do not find support for this in the supplied filings.", "citations": []}
```

Run the evaluation harness (retrieval recall@k, citation validity, refusal accuracy) against the seed question set:

```bash
pytest tests/test_rag.py::TestEvaluationHarness -v
```

### Backfill historical data

```bash
python scripts/backfill.py \
  --cik 0000320193 \
  --start-date 2020-01-01 \
  --end-date 2024-01-01
```

Progress checkpoints to `.backfill_checkpoint.json` after every successfully
processed filing, so a crash or `Ctrl-C` mid-run doesn't lose completed work:

```bash
# Resume: skip accessions already completed for this CIK.
python scripts/backfill.py --cik 0000320193 --start-date 2020-01-01 --resume

# Incremental: only fetch filings newer than the last one this CIK
# completed, ignoring --start-date for that CIK.
python scripts/backfill.py --cik 0000320193 --since-last-run
```

### Analytics: revenue trend and YoY growth by company

`financial_facts` is an OLTP table — one row per fact per filing — so "revenue
trend for AAPL over 5 years" means hand-writing a pivot query against it.
Two SQL views (`migrations/versions/0002_analytics_marts.py`) close that gap:

```sql
-- fct_company_year: one row per (cik, fiscal_year), target facts pivoted into columns
SELECT fiscal_year, revenue, net_income, total_assets
FROM fct_company_year
WHERE cik = '320193'
ORDER BY fiscal_year;

-- fct_company_year_yoy: same, plus prior-year values and YoY growth %
SELECT fiscal_year, revenue, revenue_yoy_growth_pct, net_income_yoy_growth_pct
FROM fct_company_year_yoy
WHERE cik = '320193'
ORDER BY fiscal_year;
```

Both are plain views, not materialized — always consistent with
`financial_facts` on read, no refresh job needed at this data volume.

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

## Test Coverage

169 tests, full suite runs in ~6 seconds, no external services required
(in-memory SQLite, mocked Redis, `MOCK_EDGAR=true` fixture data):

| Module | Tests | What it covers |
|---|---|---|
| `test_api.py` | 36 | FastAPI endpoints, Redis cache-then-DB behavior, OpenAPI schema |
| `test_parser.py` | 34 | XBRL extraction — units, periods, segments, amendment supersession |
| `test_quality.py` | 28 | Completeness thresholds, PSI drift detection edge cases |
| `test_dag.py` | 23 | DAG task wiring, mock-mode task callables, alerting trigger rule |
| `test_rag.py` | 15 | Chunking, TF-IDF retrieval, citation-first QA, evaluation harness |
| `test_client.py` | 14 | Rate limiting, retry/backoff, 429/503 handling |
| `test_load_idempotency.py` | 6 | UPSERT dedup on retry/reprocessing, NULL-safety |
| `test_analytics_marts.py` | 5 | `fct_company_year(_yoy)` views against the real migration |
| `test_backfill.py` | 8 | Checkpoint persistence, `--resume` / `--since-last-run` filtering |

Structural scale, for context: 7-task Airflow DAG, 6 FastAPI endpoints,
2 Alembic migrations, 4 warehouse tables + 2 analytics views. There is
no CI pipeline configured yet — `pytest tests/ -v` passing locally is the
current bar for a change being considered done (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## Testing

```bash
pytest tests/ -v                     # full suite
pytest tests/test_api.py -v          # endpoints + caching behavior
pytest tests/test_client.py -v       # rate limiting, retry/backoff
pytest tests/test_parser.py -v       # XBRL extraction, units, periods
pytest tests/test_quality.py -v      # completeness + PSI edge cases
pytest tests/test_dag.py -v          # DAG structure and task wiring
pytest tests/test_rag.py -v          # chunking, retrieval, citation-first QA, eval harness
pytest tests/test_load_idempotency.py -v  # idempotent UPSERT into financial_facts
pytest tests/test_backfill.py -v     # backfill checkpoint persistence, --resume/--since-last-run filtering
pytest tests/test_analytics_marts.py -v  # fct_company_year / fct_company_year_yoy views (real migration, real SQL)
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
| `OPENAI_API_KEY` | unset | Optional — enables LLM-phrased answers in `/ask`. Without it, answers are extractive (quoted directly from the retrieved chunk); this is the default and what tests exercise |

## Key Design Decisions

- **No LLM in the extraction path.** XBRL parsing is deterministic (`lxml`), so extraction is reproducible and carries no hallucination risk.
- **Append-only audit trail.** `pipeline_audit` is never updated or deleted from — it's a permanent record of every pipeline run.
- **Cache-first API.** Every read endpoint checks Redis first and falls back to PostgreSQL, with graceful degradation if Redis is unavailable.
- **Rate-limited ingestion.** A token-bucket limiter keeps requests to SEC EDGAR at or below 10 req/s, per their access guidelines.
- **Drift-aware quality gates.** PSI (Population Stability Index) on extracted fact distributions flags data quality regressions before they reach the warehouse, not after.
- **Retrieval, not a chatbot.** `/ask` refuses to answer (`grounded: false`) when nothing in the indexed filings scores above a relevance threshold, instead of always producing a confident-sounding response. See "Known failure modes" below for where this still falls short.
- **TF-IDF over embeddings for retrieval.** The corpus size this project targets (a scoped watchlist of tickers, not all of EDGAR) doesn't need a vector index or GPU dependency. TF-IDF is deterministic, runs offline, and keeps the test suite fast and network-free.

## Known Failure Modes and Mitigations

| Failure mode | Cause | Mitigation |
|---|---|---|
| Out-of-scope questions about a *different* company can score above the grounding threshold | Pure lexical retrieval matches shared financial boilerplate ("fiscal 2024", "net sales") even when the entity is wrong | `/ask/{ticker}` scopes the index to one ticker's ingested filings, so cross-entity confusion mostly can't occur through the API; it remains a real limitation of the retriever used in isolation (see `AGENTS.md`) |
| Citation snippet doesn't contain the supporting sentence | Naive truncation from the start of a chunk cuts off the fact past a heading/boilerplate opener | Citations are built around the sentence with the highest term overlap with the question, not the chunk's first sentence (`_best_sentence_window` in `src/rag/qa.py`) |
| `download_raw_documents` currently persists the filing *index* page, not the primary document body | Simplification in the initial ingestion implementation | Tracked as a known gap — `/ask` and the RAG tests operate on realistic narrative fixtures; production `raw_html` content needs this fixed before `/ask` returns useful answers against real ingested data |
| Retrieval quality degrades on very small corpora (few ingested filings for a ticker) | TF-IDF needs enough documents for IDF weighting to be meaningful | `max_df` filtering was intentionally left disabled to avoid `sklearn` errors on single-chunk corpora; expect noisier ranking until a ticker has several filings ingested |

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

**Status:** Core pipeline complete (ingestion, parsing, quality, caching, API, DAG, tests); citation-grounded retrieval layer (`/ask`) added | **Last updated:** August 2026
