# AGENTS.md — SEC EDGAR Extraction Pipeline

Architecture spec and build notes for contributors (human or AI) working on this repository. This project is a **data engineering / ingestion system** — there is no model training, fine-tuning, or LLM in the extraction path.

## Overview

**Purpose:** Ingest SEC 10-K/10-Q filings from EDGAR, extract structured financial facts from XBRL, validate data quality, and serve the results through a cached API — all orchestrated by Airflow.

**Status:** Core pipeline implemented — EDGAR client, XBRL parser, schema + migrations, quality checks, Redis cache, Airflow DAG, FastAPI layer, and test suite are all in place. See [README.md](README.md) for setup and usage.

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── dags/
│   └── edgar_pipeline.py       # Airflow DAG
├── src/
│   ├── edgar_client.py         # EDGAR API wrapper (rate-limited, retry)
│   ├── xbrl_parser.py          # XBRL/HTML fact extraction
│   ├── schema.py               # SQLAlchemy models
│   ├── quality.py              # completeness + PSI drift checks
│   ├── cache.py                # Redis caching layer
│   └── alerts.py               # Slack/SMTP alerting hooks
├── api/
│   └── main.py                 # FastAPI serving layer
├── scripts/
│   ├── backfill.py             # historical ingestion CLI
│   └── validate.py             # manual quality-check CLI
├── migrations/                  # Alembic environment + versions
├── tests/
│   ├── test_client.py
│   ├── test_parser.py
│   ├── test_quality.py
│   ├── test_dag.py
│   └── test_api.py
├── docker-compose.yml           # Postgres + Redis (local)
├── requirements.txt
└── AGENTS.md                    # this file
```

## Database Schema

```sql
-- Raw landing zone
CREATE TABLE filings_raw (
  accession_number TEXT PRIMARY KEY,
  cik TEXT NOT NULL,
  ticker TEXT,
  filing_type TEXT NOT NULL,          -- 10-K, 10-Q
  filing_date DATE NOT NULL,
  period_of_report DATE,
  raw_html TEXT,
  raw_xbrl TEXT,
  file_size_bytes INT,
  ingested_at TIMESTAMPTZ DEFAULT now(),
  pipeline_run_id TEXT                -- links to Airflow run
);

-- Parsed fact table
CREATE TABLE financial_facts (
  id SERIAL PRIMARY KEY,
  accession_number TEXT REFERENCES filings_raw(accession_number),
  fact_name TEXT NOT NULL,            -- e.g. us-gaap:Revenues
  fact_value NUMERIC,
  unit TEXT,
  period_start DATE,
  period_end DATE,
  segment TEXT,
  parsed_at TIMESTAMPTZ DEFAULT now()
);

-- Version history (immutable)
CREATE TABLE filing_versions (
  id SERIAL PRIMARY KEY,
  cik TEXT NOT NULL,
  filing_type TEXT NOT NULL,
  period_of_report DATE NOT NULL,
  accession_number TEXT,
  is_amendment BOOLEAN DEFAULT false,
  superseded_by TEXT,                 -- accession_number of amendment
  recorded_at TIMESTAMPTZ DEFAULT now()
);

-- Audit trail (append-only — never UPDATE/DELETE)
CREATE TABLE pipeline_audit (
  id SERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  stage TEXT NOT NULL,                -- ingest | parse | validate | load
  status TEXT NOT NULL,               -- started | completed | failed
  records_processed INT,
  error_message TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
```

## Component Notes

### `edgar_client.py`
- Submissions: `https://data.sec.gov/submissions/CIK{cik:010d}.json`
- XBRL facts: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json`
- Full-text search: `https://efts.sec.gov/LATEST/search-index?q=...`
- SEC rate limit: 10 requests/second — enforced with a token-bucket limiter.
- A `User-Agent` header is required by SEC; requests without one return 403.
- Retries on 429/503 with exponential backoff, max 5 attempts.

### `xbrl_parser.py`
Extracts a fixed set of target facts: `us-gaap:Revenues` (or `RevenueFromContractWithCustomerExcludingAssessedTax`), `NetIncomeLoss`, `Assets`, `Liabilities`, `OperatingIncomeLoss`, `EarningsPerShareBasic`, `CommonStockSharesOutstanding`. Handles instant vs. duration periods, unit conversion (thousands vs. millions), segment disaggregation, and amendment supersession (latest amendment wins).

### `dags/edgar_pipeline.py`
Seven-task linear DAG:

```
fetch_new_filings → download_raw_documents → parse_xbrl_facts
  → validate_quality_gates → load_to_warehouse
  → update_audit_trail → send_alerts_on_failure
```

Each stage writes a start/end row to `pipeline_audit`. `validate_quality_gates` raises `AirflowSkipException` when there are no new filings, or `AirflowFailException` when quality gates fail.

### `quality.py`
- **Completeness:** fails a run if fewer than a configurable threshold (default 95%) of filings have all required facts populated.
- **PSI drift:** Population Stability Index on `fact_value` distributions, current run vs. a rolling baseline. `<0.1` clean, `0.1–0.25` warn, `>0.25` alert.

### `cache.py`
Redis key patterns: `cik:{ticker}` (TTL 1h), `filings:{cik}` (TTL 24h), `facts:{accession_number}` (TTL 7d — parsed facts don't change).

### `api/main.py`
```
GET  /health
GET  /filings/{ticker}            list filings for a ticker (paginated)
GET  /filing/{accession}          parsed facts for one filing
GET  /facts/{ticker}/{fact_name}  time-series of a specific fact
POST /trigger/{ticker}             trigger on-demand ingestion
```
All read endpoints check Redis before PostgreSQL and return typed Pydantic models. Unknown ticker/accession returns 404 with a detail message.

## Testing Strategy

- **Unit:** mocked SEC API responses (`requests-mock`), fixture XBRL HTML for the parser, PSI/completeness edge cases.
- **Integration:** DAG structure/wiring tests, `TestClient`-based API tests against an in-memory SQLite DB and mocked Redis.
- Set `MOCK_EDGAR=true` to run the full suite without live network calls.

## Common Pitfalls

| Issue | Solution |
|---|---|
| 403 from SEC EDGAR | Missing or malformed `User-Agent` header — must identify the app and a contact email |
| 429/503 from SEC EDGAR | Token-bucket limiter or backoff misconfigured — verify `RATE_LIMIT_RPS` and retry count |
| Duplicate facts across amendments | Ensure `filing_versions.superseded_by` is respected when querying "current" facts |
| Cache staleness | Facts TTL is 7 days — invalidate manually via `cache.py` if a filing is corrected out-of-band |

## Agent Handoff Checklist

When handing this project to another contributor or agent, update this section:

**Last updated:** August 2026
**Current state:** Core pipeline complete — ingestion, parsing, schema/migrations, quality checks, cache, DAG, API, and tests are all implemented and passing.

**Next steps (not yet done):**
- CI workflow (lint/type-check/test on push)
- `.env.example` for local setup
- Production deployment guide (current `docker-compose.yml` is local-dev only; no Kubernetes/Helm config exists yet)

## Resources

- SEC EDGAR API docs: https://www.sec.gov/edgar/sec-api-documentation
- Airflow docs: https://airflow.apache.org/docs/
- FastAPI docs: https://fastapi.tiangolo.com/
