# AGENTS.md — SEC EDGAR Extraction Pipeline

Architecture spec and build notes for contributors (human or AI) working on this repository. This project is a **data engineering / ingestion system** that extracts **only iXBRL-tagged financial facts** — deterministic, machine-readable data that the SEC filer explicitly tagged. There is no model training, fine-tuning, or LLM in the extraction path.

**Scope boundary:** This repo owns tagged-XBRL extraction only. Extraction from unstructured narrative (MD&A, footnotes, non-GAAP reconciliations, untagged tables) belongs to the separate **Fine-Tuned-SEC-Filing-Extraction-Pipeline** repository. See [docs/BOUNDARY.md](docs/BOUNDARY.md) for the full scope contract, precedence rules, and handoff surface between the two repos.

## Overview

**Purpose:** Ingest SEC 10-K/10-Q filings from EDGAR, extract structured financial facts from iXBRL tags, validate data quality, and serve the results through a cached API — all orchestrated by Airflow. Every fact this pipeline produces is deterministically parsed from machine-tagged XBRL data; no fact is ever inferred from prose.

**Status:** Core pipeline implemented — EDGAR client, XBRL parser, schema + migrations, quality checks, Redis cache, Airflow DAG, FastAPI layer, and test suite are all in place. A lightweight filing-text lookup aid (`src/rag/`, served via `GET /ask/{ticker}`) sits on top of the structured pipeline as a read-only research tool for navigating filing prose — it does not extract structured facts and is not the narrative extraction system (see [docs/BOUNDARY.md](docs/BOUNDARY.md)).

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── docs/
│   └── BOUNDARY.md              # Scope contract: this repo vs. the LLM extraction repo
├── dags/
│   └── edgar_pipeline.py       # Airflow DAG
├── src/
│   ├── edgar_client.py         # EDGAR API wrapper (rate-limited, retry)
│   ├── xbrl_parser.py          # iXBRL tag extraction (deterministic, no ML)
│   ├── schema.py               # SQLAlchemy models
│   ├── quality.py              # completeness + PSI drift checks
│   ├── cache.py                # Redis caching layer
│   ├── alerts.py               # Slack/SMTP alerting hooks
│   └── rag/                    # Filing-text lookup aid (read-only, not extraction)
│       ├── chunker.py           # Filing text -> provenance-tagged chunks
│       ├── retrieval.py         # TF-IDF retrieval over chunks
│       ├── qa.py                # Citation-first answer construction, refusal behavior
│       └── evaluation.py        # Retrieval recall / citation validity / grounding-accuracy harness
├── api/
│   └── main.py                 # FastAPI serving layer (incl. GET /ask/{ticker})
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
Deterministic iXBRL tag extraction — no model inference, no prose parsing. Extracts a fixed set of target facts: `us-gaap:Revenues` (or `RevenueFromContractWithCustomerExcludingAssessedTax`), `NetIncomeLoss`, `Assets`, `Liabilities`, `OperatingIncomeLoss`, `EarningsPerShareBasic`, `CommonStockSharesOutstanding`. Handles instant vs. duration periods, unit conversion (thousands vs. millions), segment disaggregation, and amendment supersession (latest amendment wins). Only processes machine-tagged XBRL data; numbers embedded in narrative prose are out of scope (see [docs/BOUNDARY.md](docs/BOUNDARY.md)).

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

### `src/rag/` — filing-text lookup aid (read-only, not extraction)

> **Scope note:** This is a lightweight read-only research tool for navigating filing prose — it helps a human find a relevant passage. It is **not** the narrative extraction system described in [docs/BOUNDARY.md](docs/BOUNDARY.md). It does not extract structured facts, does not write to `financial_facts`, and does not produce data that feeds downstream. The Fine-Tuned-SEC-Filing-Extraction-Pipeline repo owns all extraction from unstructured text.

Chunks filing text into Item-level sections (`chunker.py`), retrieves the most relevant chunks for a question via TF-IDF (`retrieval.py`), and constructs a citation-first answer (`qa.py`). Deliberately **not** an embedding-based system: the target corpus size (a scoped watchlist, not all of EDGAR) doesn't need one, and TF-IDF keeps retrieval deterministic, offline, and fast to test.

Key behaviors, and why they exist:

- **Refusal, not confident guessing.** `answer_question()` returns `grounded=False` and a fixed refusal string when the best retrieval score is below `MIN_GROUNDING_SCORE` (0.15). This threshold was tuned empirically against the narrative fixtures in `tests/fixtures/sample_filing_narrative_10k.html` / `_10q.html` — see the test history in `tests/test_rag.py` for the calibration process, including a real bug it caught (see below).
- **Domain-aware stopwords.** SEC boilerplate ("fiscal", "the Company", "this Item") appears in nearly every chunk of nearly every filing and would otherwise dominate cosine similarity for out-of-scope questions that happen to mention a fiscal year. `_FILING_BOILERPLATE_STOPWORDS` in `retrieval.py` strips these before vectorizing.
- **Sentence-level citation windows, not chunk-start truncation.** A naive `text[:280]` snippet shows the section heading and opening boilerplate, not the sentence that actually supports the answer. `_best_sentence_window()` in `qa.py` picks the sentence with the highest term overlap with the question and starts the snippet there.
- **Optional LLM synthesis, never required.** If `OPENAI_API_KEY` is set, `_try_llm_synthesis()` asks a model to phrase the answer using *only* the retrieved chunk text. Any failure (missing key, network error, bad response) falls back to the extractive path silently — this keeps `/ask` and its tests fully offline by default. This is answer *phrasing*, not fact extraction — it never produces structured data or writes to any table.

**Known limitation — entity disambiguation.** A lexical retriever cannot reliably tell "this is about a different company" from "this happens to share financial boilerplate vocabulary." Early testing found "What was Tesla's revenue in fiscal 2024?" scoring *above* the grounding threshold against an Apple-only corpus, purely on the strength of the shared phrase "fiscal 2024." This is mitigated at the API layer: `GET /ask/{ticker}` only ever indexes filings for the requested ticker, so cross-entity confusion mostly can't occur in practice — but the retriever module in isolation (as exercised directly in `tests/test_rag.py`) doesn't solve this itself, and its evaluation cases were revised to test genuine topic-absence (e.g. "return policy for damaged retail products") rather than cross-entity mixups, which are a different, harder problem (would need NER/entity resolution or semantic embeddings, not lexical matching).

**Known limitation — `raw_html` is currently the filing index page, not the primary document.** `download_raw_documents` in `dags/edgar_pipeline.py` fetches the accession's directory-listing HTML, not the actual 10-K/10-Q document body. `/ask` and the lookup-aid tests are validated against realistic narrative fixtures, but will need this ingestion gap closed before returning useful results against real production data. Fixing this is a data-engineering task (locate and fetch the primary document filename from the index, not the index itself), independent of the lookup layer.

### `api/main.py`
```
GET  /health
GET  /filings/{ticker}            list filings for a ticker (paginated)
GET  /filing/{accession}          parsed XBRL facts for one filing
GET  /facts/{ticker}/{fact_name}  time-series of a specific XBRL fact
GET  /ask/{ticker}                filing-text lookup aid (read-only, returns quoted prose + citation)
POST /trigger/{ticker}             trigger on-demand ingestion
```
All read endpoints check Redis before PostgreSQL and return typed Pydantic models. Unknown ticker/accession returns 404 with a detail message. `/ask` is a read-only lookup tool — it retrieves and quotes filing prose for human review but does not extract structured facts or write to `financial_facts` (see [docs/BOUNDARY.md](docs/BOUNDARY.md)). It builds its retrieval index on demand from `filings_raw.raw_html` per request rather than caching it — acceptable at the corpus sizes this project targets; worth revisiting (e.g. cache the fitted retriever in Redis, invalidate on new filings) if ticker filing counts grow large enough for TF-IDF fitting to become a latency concern.

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
**Scope:** This repo owns tagged-XBRL extraction only. Read [docs/BOUNDARY.md](docs/BOUNDARY.md) before making changes — it defines what belongs here vs. in the Fine-Tuned-SEC-Filing-Extraction-Pipeline repo.
**Current state:** Core pipeline complete — ingestion, iXBRL parsing, schema/migrations, quality checks, cache, DAG, API, and tests are all implemented and passing. A lightweight filing-text lookup aid (`src/rag/`, `GET /ask/{ticker}`) is included for navigating filing prose — it is read-only and does not extract structured facts.

**Next steps (not yet done):**
- Fix `download_raw_documents` to persist the primary filing document, not the index page (blocks the lookup aid from being useful against real ingested data, and blocks the LLM repo from consuming real filing text — see "Known limitation" under `src/rag/` above)
- Grow the lookup-aid evaluation set from 10 seed cases toward the 25-50 recommended for a credible eval
- Add a `method` column to `financial_facts` (+ `confidence`, `model_version`) when the LLM repo is ready to write facts — see [docs/BOUNDARY.md](docs/BOUNDARY.md) for the precedence rule
- CI workflow (lint/type-check/test on push)
- `.env.example` for local setup
- Production deployment guide (current `docker-compose.yml` is local-dev only; no Kubernetes/Helm config exists yet)
- There is separate, uncommitted work in progress on load-stage idempotency (`_upsert_facts` in `dags/edgar_pipeline.py`, a unique expression index in `src/schema.py`, plus `tests/test_load_idempotency.py`) — unrelated to the lookup aid, not touched by it, and not yet committed as of this update

## Resources

- SEC EDGAR API docs: https://www.sec.gov/edgar/sec-api-documentation
- Airflow docs: https://airflow.apache.org/docs/
- FastAPI docs: https://fastapi.tiangolo.com/
