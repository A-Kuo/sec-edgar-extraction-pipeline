# SEC EDGAR Extraction Pipeline

**Deterministic financial fact extraction from iXBRL-tagged SEC filings**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![Status](https://img.shields.io/badge/Status-Production--Grade-brightgreen.svg)]()

> *"Every number that matters in a 10-K has already been tagged — by the company itself, in machine-readable XBRL, filed under penalty of law. The question is not whether you can extract it. The question is whether you can do it at scale, handle amendments, catch data quality regressions, and serve it fast enough to be useful."*

---

## The Problem: Why EDGAR Extraction Is Harder Than It Looks

The SEC's EDGAR database holds over 20 million filings from tens of thousands of companies. Since 2009, the SEC has required filers to tag key financial figures in iXBRL (Inline eXtensible Business Reporting Language) — machine-readable markup embedded directly in the HTML filing. In theory, this means every revenue figure, every net income number, every balance sheet total is already structured data waiting to be read.

In practice, building a reliable pipeline from raw EDGAR to queryable financial time-series is a multi-faceted engineering problem:

**1. Rate limiting and access control.** SEC EDGAR enforces a 10-request-per-second rate limit and requires a specific `User-Agent` header identifying the caller. Exceed the limit and you get 429s. Omit the header and you get 403s. A naive `requests.get()` loop will be blocked within seconds.

**2. Amendment chains create duplicates.** When a company files a 10-K/A (amended annual report), it supersedes the original filing. A pipeline that doesn't track amendment chains will double-count or serve stale figures. Apple alone has filed amendments that change reported revenue figures by billions.

**3. Unit inconsistency across filings.** One filing reports revenue with `scale="6"` (millions), another with `scale="3"` (thousands), a third with no scale attribute at all. The same number — $394.3 billion — appears as `394328`, `394328000`, or `394328000000` depending on the filer's XBRL tagging choices.

**4. Period semantics are not uniform.** Balance sheet items are "instant" (a snapshot at a date). Income statement items are "duration" (a range between two dates). The same filing contains both, tagged in different XBRL contexts. Mix them up and you get nonsensical time-series.

**5. Data quality regressions are silent.** When a filer changes their XBRL tagging taxonomy between years, or when a pipeline bug introduces systematic extraction errors, the data *looks* correct — you just get slightly wrong numbers. Without statistical drift detection, these regressions go unnoticed until someone downstream builds a model on corrupted data.

**6. Filings are large and slow to process.** A single 10-K can be 15 MB of HTML. Parsing XBRL contexts, resolving unit references, and extracting facts from 50+ inline elements takes real compute. At portfolio scale (hundreds of companies, quarterly), you need orchestration, checkpointing, and parallelism — not a script that runs once.

This pipeline solves all six problems with zero ML dependencies: deterministic `lxml` parsing, token-bucket rate limiting, amendment-aware version history, unit normalization, PSI-based drift detection, and Airflow orchestration. Every fact it produces is traceable to a specific XBRL tag in a specific accession number.

---

## At a Glance

| Metric | Value |
|--------|-------|
| Extraction method | Deterministic iXBRL tag parsing (`lxml`) — no ML, no inference |
| Target facts | 7 core financial metrics (revenue, net income, assets, liabilities, operating income, EPS, shares outstanding) |
| Orchestration | 7-task Airflow DAG with per-stage audit trail |
| Storage | PostgreSQL (4 tables + 2 analytics views) with Redis cache |
| Quality gates | Completeness threshold (95%) + PSI drift detection |
| API endpoints | 6 (health, filings, filing, facts, ask, trigger) |
| Test suite | 169 tests, ~3 seconds, no external services |
| Infrastructure | PostgreSQL 16 + Redis 7 via Docker Compose |

---

## How It Works

### The Extraction Path

Every fact this pipeline produces follows the same deterministic path — no model inference, no heuristics, no confidence scores. If the SEC filer tagged it in iXBRL, we extract it. If they didn't, we don't.

```
SEC EDGAR API
      │
      │  Rate-limited (10 req/s), User-Agent required
      │  Retry on 429/503 with exponential backoff
      ▼
┌──────────────────────────────────────────────────────────┐
│  Airflow DAG: edgar_pipeline  (7 tasks, linear chain)    │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  1. fetch_new_filings ─── Query EDGAR for new 10-K/10-Q │
│         │                                                │
│  2. download_raw_documents ─── Persist raw HTML to DB    │
│         │                                                │
│  3. parse_xbrl_facts ─── Extract tagged facts via lxml   │
│         │                   ├── Unit normalization        │
│         │                   ├── Period type resolution    │
│         │                   └── Segment disaggregation   │
│         │                                                │
│  4. validate_quality_gates                               │
│         │   ├── Completeness ≥ 95% of required facts     │
│         │   └── PSI drift < 0.25 vs. rolling baseline    │
│         │                                                │
│  5. load_to_warehouse ─── Idempotent UPSERT to Postgres │
│         │                                                │
│  6. update_audit_trail ─── Append-only (never UPDATE)    │
│         │                                                │
│  7. send_alerts_on_failure ─── Slack / SMTP / stderr     │
│                                                          │
└──────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────┐    ┌──────────────────────────────┐
│  PostgreSQL          │    │  Redis Cache                  │
│  ┌─ filings_raw     │    │  ┌─ CIK lookup     (TTL 1h)  │
│  ├─ financial_facts  │◄───┤  ├─ Filing index   (TTL 24h) │
│  ├─ filing_versions  │    │  └─ Parsed facts   (TTL 7d)  │
│  └─ pipeline_audit   │    └──────────────────────────────┘
└─────────────────────┘
      │
      ▼
┌──────────────────────────────────────────────────────────┐
│  FastAPI Serving Layer                                    │
│  GET  /health              ─── Liveness probe             │
│  GET  /filings/{ticker}    ─── Filing index (paginated)   │
│  GET  /filing/{accession}  ─── XBRL facts for one filing  │
│  GET  /facts/{ticker}/{f}  ─── Time-series of one fact    │
│  GET  /ask/{ticker}        ─── Filing-text lookup aid      │
│  POST /trigger/{ticker}    ─── On-demand ingestion         │
└──────────────────────────────────────────────────────────┘
```

### The iXBRL Parser

The parser (`src/xbrl_parser.py`) targets 7 core financial concepts:

| XBRL Concept | What It Is |
|---|---|
| `us-gaap:Revenues` | Total revenue (or `RevenueFromContractWithCustomerExcludingAssessedTax`) |
| `us-gaap:NetIncomeLoss` | Bottom-line net income |
| `us-gaap:Assets` | Total assets (balance sheet) |
| `us-gaap:Liabilities` | Total liabilities (balance sheet) |
| `us-gaap:OperatingIncomeLoss` | Operating income |
| `us-gaap:EarningsPerShareBasic` | Basic EPS |
| `us-gaap:CommonStockSharesOutstanding` | Share count |

For each fact, the parser resolves:

- **Period type** — instant (balance sheet snapshot) vs. duration (income statement range)
- **Scale attribute** — `scale="6"` means multiply by 10⁶; `scale="3"` by 10³
- **Sign attribute** — `sign="-"` negates the value; parenthesized values `(123)` → `-123`
- **Unit reference** — ISO 4217 currency codes, shares, or ratio units (USD/share)
- **Segment** — consolidated (NULL) vs. named business segment
- **Amendment supersession** — latest amendment wins for the same (fact, period, segment) key

### Quality Gates

Before any extracted facts reach the warehouse, two statistical checks must pass:

**Completeness check** — at least 95% of filings in a pipeline run must have all three required facts (Revenue, Net Income, Assets) populated. A filing that's missing a required fact is a signal that the parser hit an edge case — better to fail the run and investigate than to silently load partial data.

**PSI drift detection** — the Population Stability Index compares the distribution of extracted fact values in the current run against a rolling baseline. PSI thresholds follow industry standard: `< 0.10` clean, `0.10–0.25` warn, `> 0.25` alert. A sudden spike in PSI means something changed — a filer switched taxonomy, a parser bug was introduced, or an upstream data source shifted.

```
PSI < 0.10   →  CLEAN   (no action)
PSI 0.10–0.25 →  WARN   (logged, pipeline continues)
PSI > 0.25   →  ALERT   (pipeline fails, alert dispatched)
```

---

## Database Schema

Four tables, two analytics views, one rule: the audit trail is append-only.

```sql
filings_raw          -- Landing zone: raw HTML/XBRL, one row per accession
financial_facts      -- Parsed iXBRL facts, FK to filings_raw
filing_versions      -- Amendment chain tracking (superseded_by)
pipeline_audit       -- Append-only stage-level audit trail (never UPDATE/DELETE)

fct_company_year     -- View: pivoted facts per (cik, fiscal_year)
fct_company_year_yoy -- View: same + prior-year values and YoY growth %
```

The `financial_facts` table uses a unique expression index on `(accession_number, fact_name, COALESCE(period_end, '1900-01-01'), COALESCE(segment, ''))` — this makes the load stage idempotent via `INSERT ... ON CONFLICT DO UPDATE`, so Airflow retries and re-processed amendments don't create duplicate rows.

---

## Integration with the LLM Extraction Pipeline

This pipeline is the **tagged-data half** of a two-repo extraction system. The seam between them is whether the SEC filer machine-tagged the number:

```
                    ┌──────────────────────────────────┐
SEC EDGAR ─────────►│  sec-edgar-extraction-pipeline    │
  (raw filings)     │  (this repo)                      │
                    │                                    │
                    │  Extracts: iXBRL-tagged facts      │
                    │  Method:   deterministic lxml      │
                    │  Output:   method='xbrl'           │
                    └──────────┬───────────────────────┘
                               │
                               │  filings_raw (documents)
                               │  financial_facts (for precedence)
                               │  filing_versions (amendment chains)
                               ▼
                    ┌──────────────────────────────────┐
                    │  Fine-Tuned-SEC-Filing-Extraction │
                    │  -Pipeline (companion repo)       │
                    │                                    │
                    │  Extracts: narrative prose facts    │
                    │  Method:   QLoRA Llama 3.1 8B      │
                    │  Output:   method='llm' + conf     │
                    └──────────────────────────────────┘
```

**Precedence rule:** An `llm` fact never overwrites an `xbrl` fact for the same natural key `(accession_number, fact_name, period_end, segment)`. XBRL always wins — the filer tagged it under penalty of law. The reverse is permitted: an `xbrl` fact arriving after an `llm` fact replaces it.

The full scope contract — what this repo emits, what the LLM repo consumes, what neither repo should duplicate — is documented in [docs/BOUNDARY.md](docs/BOUNDARY.md).

---

## Setup

```bash
git clone https://github.com/A-Kuo/sec-edgar-extraction-pipeline.git
cd sec-edgar-extraction-pipeline

pip install -r requirements.txt

# Start PostgreSQL + Redis
docker-compose up -d

# Apply schema migrations
alembic upgrade head

# Run the test suite (no external services required)
MOCK_EDGAR=true pytest tests/ -v

# Start the API
uvicorn api.main:app --reload --port 8000
# Interactive docs at http://localhost:8000/docs
```

> **Note:** `SEC_USER_AGENT` is required for live EDGAR access. SEC returns 403 without it. Set `MOCK_EDGAR=true` for local development to use fixture data instead.

---

## Usage

### Query XBRL Facts via API

All read endpoints check Redis before hitting PostgreSQL. Cache misses populate Redis transparently.

```bash
# Health check
curl http://localhost:8000/health

# List filings for a ticker (paginated)
curl http://localhost:8000/filings/AAPL?limit=10&offset=0

# Get parsed XBRL facts for one filing
curl http://localhost:8000/filing/0000320193-23-000077
```

Response:
```json
{
  "accession_number": "0000320193-24-000123",
  "filing_type": "10-K",
  "period_of_report": "2024-09-28",
  "facts": [
    {
      "fact_name": "us-gaap:Revenues",
      "fact_value": 391035000000.0,
      "unit": "USD",
      "period_start": "2023-10-01",
      "period_end": "2024-09-28",
      "segment": null
    }
  ]
}
```

```bash
# Time-series of a specific fact across all filings
curl http://localhost:8000/facts/AAPL/us-gaap:Revenues

# Trigger on-demand ingestion
curl -X POST http://localhost:8000/trigger/AAPL
```

### Filing-Text Lookup (`/ask`)

A read-only research aid that retrieves and quotes relevant passages from filing text. This is a keyword-based lookup tool — not an extraction endpoint. It does not produce structured facts or write to `financial_facts`.

```bash
curl -G http://localhost:8000/ask/AAPL \
  --data-urlencode "q=What supply chain risk does the company describe?"
```

```json
{
  "ticker": "AAPL",
  "question": "What supply chain risk does the company describe?",
  "answer": "Based on 10-K 0000320193-24-000123 (Item 1A. Risk Factors): The Company relies on single-source and limited-source suppliers...",
  "grounded": true,
  "citations": [
    {
      "accession_number": "0000320193-24-000123",
      "section": "Item 1A. Risk Factors",
      "snippet": "The Company relies on single-source and limited-source suppliers...",
      "score": 0.34
    }
  ]
}
```

When nothing in the indexed filings is relevant, the endpoint refuses instead of guessing:

```json
{"grounded": false, "answer": "I do not find support for this in the supplied filings.", "citations": []}
```

### Backfill Historical Data

```bash
python scripts/backfill.py \
  --cik 0000320193 \
  --start-date 2020-01-01 \
  --end-date 2024-01-01
```

Progress checkpoints to `.backfill_checkpoint.json` after every successfully processed filing, so a crash mid-run doesn't lose completed work:

```bash
# Resume from where you left off
python scripts/backfill.py --cik 0000320193 --start-date 2020-01-01 --resume

# Only fetch filings newer than the last completed one
python scripts/backfill.py --cik 0000320193 --since-last-run
```

### Analytics Views

Two SQL views close the gap between the OLTP fact table and analytical queries:

```sql
-- Revenue trend for Apple over 5 years
SELECT fiscal_year, revenue, net_income, total_assets
FROM fct_company_year
WHERE cik = '320193'
ORDER BY fiscal_year;

-- Year-over-year growth
SELECT fiscal_year, revenue, revenue_yoy_growth_pct, net_income_yoy_growth_pct
FROM fct_company_year_yoy
WHERE cik = '320193'
ORDER BY fiscal_year;
```

### Airflow DAG

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow dags unpause edgar_pipeline
airflow dags test edgar_pipeline 2024-01-15
```

---

## Testing

169 tests, full suite runs in ~3 seconds, no external services required (in-memory SQLite, mocked Redis, `MOCK_EDGAR=true` fixture data):

```bash
MOCK_EDGAR=true pytest tests/ -v          # full suite
pytest --cov=src --cov=api tests/         # with coverage
```

| Module | Tests | Focus |
|--------|-------|-------|
| `test_api.py` | 36 | FastAPI endpoints, Redis cache-then-DB behavior, OpenAPI schema |
| `test_parser.py` | 34 | iXBRL extraction — units, periods, segments, amendment supersession |
| `test_quality.py` | 28 | Completeness thresholds, PSI drift detection edge cases |
| `test_dag.py` | 23 | DAG task wiring, mock-mode task callables, alerting trigger rule |
| `test_rag.py` | 15 | Filing-text lookup: chunking, TF-IDF retrieval, citation QA, eval harness |
| `test_client.py` | 14 | Rate limiting, retry/backoff, 429/503 handling |
| `test_backfill.py` | 8 | Checkpoint persistence, `--resume` / `--since-last-run` filtering |
| `test_load_idempotency.py` | 6 | UPSERT dedup on retry/reprocessing, NULL-safety |
| `test_analytics_marts.py` | 5 | `fct_company_year(_yoy)` views against real migration SQL |

---

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── docs/
│   └── BOUNDARY.md              # Scope contract: this repo vs. the LLM extraction repo
├── src/
│   ├── edgar_client.py          # EDGAR API client (rate-limited, retry with backoff)
│   ├── xbrl_parser.py           # iXBRL tag extraction (deterministic, no ML)
│   ├── schema.py                # SQLAlchemy ORM models (4 tables)
│   ├── quality.py               # Completeness checks + PSI drift detection
│   ├── cache.py                 # Redis caching (CIK, filing index, facts)
│   ├── alerts.py                # Slack/SMTP alerting on pipeline failure
│   └── rag/                     # Filing-text lookup aid (read-only, not extraction)
│       ├── chunker.py            # Filing text → provenance-tagged chunks
│       ├── retrieval.py          # TF-IDF retrieval over chunks
│       ├── qa.py                 # Citation-first answer construction
│       └── evaluation.py         # Retrieval recall / citation validity harness
├── api/
│   └── main.py                  # FastAPI serving layer
├── dags/
│   └── edgar_pipeline.py        # Airflow DAG (7-task pipeline)
├── scripts/
│   ├── backfill.py              # CLI: historical ingestion by CIK + date range
│   └── validate.py              # CLI: run quality checks for a given run_id
├── migrations/                   # Alembic migration environment + versions
├── tests/                        # 169 tests, ~3s, no external services
├── docker-compose.yml            # PostgreSQL 16 + Redis 7
├── requirements.txt
├── alembic.ini
├── AGENTS.md                     # Architecture spec for contributors/agents
└── CONTRIBUTING.md
```

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `postgresql://sec_user:sec_pass@localhost/sec_edgar` | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `SEC_USER_AGENT` | *(none)* | **Required** for live EDGAR access — identifies the caller per SEC policy |
| `MOCK_EDGAR` | `false` | Set `true` for local dev/tests to use fixture data instead of live API calls |
| `SLACK_WEBHOOK_URL` | *(unset)* | Optional Slack alerting on pipeline failure |
| `SMTP_HOST`, `ALERT_EMAIL_TO` | *(unset)* | Optional email alerting on pipeline failure |
| `OPENAI_API_KEY` | *(unset)* | Optional — enables LLM-phrased answers in `/ask` lookup aid. Without it, answers are extractive (quoted directly from retrieved text). The LLM only rephrases — it never extracts facts. |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Tagged-XBRL extraction only** | This repo extracts facts the filer already machine-tagged in iXBRL. Narrative extraction (MD&A, footnotes, non-GAAP tables) is a fundamentally different problem — it belongs in the [LLM extraction repo](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline). See [docs/BOUNDARY.md](docs/BOUNDARY.md). |
| **No LLM in the extraction path** | iXBRL parsing is deterministic (`lxml`). No model inference, no hallucination risk, no GPU requirement. Every fact is reproducible from the same input. |
| **Append-only audit trail** | `pipeline_audit` is never updated or deleted from — it is a permanent record of every pipeline run, every stage, every failure. |
| **Cache-first API** | Every read endpoint checks Redis before PostgreSQL, with graceful degradation if Redis is unavailable. Facts are cached for 7 days — they don't change once parsed. |
| **Rate-limited ingestion** | A token-bucket limiter enforces SEC's 10 req/s limit. Violating it gets your IP blocked — there is no recovery path except waiting. |
| **PSI-based drift detection** | Statistical quality gates catch data regressions before they reach the warehouse. A 12% shift in revenue distributions is not normal — the pipeline should stop and alert, not silently load corrupted data. |
| **Idempotent load stage** | `INSERT ... ON CONFLICT DO UPDATE` on a unique expression index means Airflow retries and re-processed amendments never create duplicate rows. |
| **TF-IDF for the lookup aid** | The filing-text search tool uses TF-IDF, not embeddings. The corpus is small (scoped watchlist), determinism matters, and adding `torch` as a dependency for a read-only lookup tool is not justified. |

---

## Known Limitations

| Limitation | Impact | Status |
|------------|--------|--------|
| `download_raw_documents` stores the filing index page, not the primary document | `/ask` and the LLM repo can't consume real filing text until this is fixed | Tracked — data-engineering task, independent of extraction logic |
| No `method` column on `financial_facts` | Can't distinguish XBRL-sourced facts from future LLM-sourced facts in the same table | Migration planned for when the LLM repo is ready to write facts |
| Lookup aid can't disambiguate entities | Lexical retrieval matches shared boilerplate across companies | Mitigated by ticker-scoped indexing in `/ask/{ticker}` — cross-entity confusion only affects the retriever in isolation |
| No CI pipeline | Tests pass locally but aren't enforced on push | Tracked as next step |

---

## Related Repositories

| Repository | Role | Relationship |
|------------|------|-------------|
| [Fine-Tuned-SEC-Filing-Extraction-Pipeline](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline) | Extracts facts from untagged narrative (MD&A, footnotes) using QLoRA fine-tuned Llama 3.1 8B | Consumes `filings_raw` and `filing_versions` from this repo; writes `method='llm'` facts |

---

## Contributing

See [AGENTS.md](AGENTS.md) for the full architecture spec, schema definitions, and implementation notes. Read [docs/BOUNDARY.md](docs/BOUNDARY.md) before adding features that could overlap with the LLM extraction repo.

```bash
git checkout -b feature/your-feature
MOCK_EDGAR=true pytest tests/ -v    # must pass
git commit -m "descriptive message"
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full guidelines.

## License

MIT License — see [LICENSE](LICENSE).

---

*The data was always machine-tagged. Making it queryable, versioned, and trustworthy at scale is the engineering. August 2026.*
