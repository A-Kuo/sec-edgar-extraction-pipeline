# AGENTS.md — SEC EDGAR Extraction Pipeline

Implementation guide for AI coding agents. This repo does not yet exist —
use this file as the build specification when creating the repository.

**Suggested repo name:** `SEC-EDGAR-Extraction-Pipeline`

Note: this is the NEW pipeline project, distinct from the old
`Fine-Tuned-SEC-Filing-Extraction-Pipeline` repo (QLoRA fine-tuning project).
This repo is purely a data engineering / ingestion system.

---

## CV Role

This repo appears on the **Data Engineer CV only**.

### Data Engineer CV — Financial Document Ingestion & Quality

**Stack line:** PostgreSQL, Airflow, FastAPI, Redis, Python, SQL

**Impact:** 10-K/10-Q ingestion to structured tables | PostgreSQL audit lineage |
automated drift checks on extracted fields

**Bullets:**
- Designed PostgreSQL schema with accession-level lineage, filing-version history,
  and immutable audit trails for downstream analytics and training datasets
- Built Airflow-orchestrated ETL from SEC EDGAR API with rate-limited ingestion,
  raw HTML/XBRL landing zone, parsed fact tables, and stage-level validation gates
- Implemented data quality monitoring (completeness thresholds, PSI drift on
  numeric facts), Redis caching for hot lookups, and alerting on pipeline failures

---

## Repo Structure to Create

```
SEC-EDGAR-Extraction-Pipeline/
├── dags/
│   └── edgar_pipeline.py         # Airflow DAG
├── src/
│   ├── edgar_client.py           # EDGAR API wrapper (rate-limited)
│   ├── xbrl_parser.py            # XBRL/HTML fact extraction
│   ├── schema.py                 # SQLAlchemy models
│   ├── quality.py                # completeness + PSI drift checks
│   ├── cache.py                  # Redis caching layer
│   └── alerts.py                 # alerting hooks
├── api/
│   └── main.py                   # FastAPI serving layer
├── scripts/
│   ├── backfill.py               # historical ingestion CLI
│   └── validate.py               # run quality checks manually
├── tests/
│   ├── test_client.py
│   ├── test_parser.py
│   ├── test_quality.py
│   └── test_dag.py
├── docker-compose.yml            # Postgres + Redis + Airflow local
├── requirements.txt
├── README.md
└── AGENTS.md                     # this file
```

---

## Implementation Specification

### Step 1 — PostgreSQL schema with lineage (CV bullet 1)

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
  accession_number TEXT,              -- which accession is current
  is_amendment BOOLEAN DEFAULT false,
  superseded_by TEXT,                 -- accession_number of amendment
  recorded_at TIMESTAMPTZ DEFAULT now()
);

-- Audit trail
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

### Step 2 — EDGAR API client (CV bullet 2)

`src/edgar_client.py`:

- Base URL: `https://data.sec.gov/submissions/CIK{cik:010d}.json`
- XBRL facts: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json`
- Full-text search: `https://efts.sec.gov/LATEST/search-index?q=...`
- SEC rate limit: 10 requests/second; User-Agent header required:
  `User-Agent: SEC-EDGAR-Pipeline aus.kuo03@gmail.com`
- Implement token bucket rate limiter
- Retry on 429/503 with exponential backoff (max 5 retries)

```python
class EdgarClient:
    def get_company_filings(self, cik: str) -> dict
    def get_filing_index(self, cik: str, accession: str) -> list[str]
    def get_filing_document(self, url: str) -> str
    def get_xbrl_facts(self, cik: str) -> dict
    def search_filings(self, query: str, form_type: str, date_range: tuple) -> list
```

### Step 3 — XBRL parser (CV bullet 2)

`src/xbrl_parser.py` extracts structured financial facts from XBRL inline data:

Target facts (minimum viable set):
- `us-gaap:Revenues` or `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax`
- `us-gaap:NetIncomeLoss`
- `us-gaap:Assets`, `us-gaap:Liabilities`
- `us-gaap:OperatingIncomeLoss`
- `us-gaap:EarningsPerShareBasic`
- `us-gaap:CommonStockSharesOutstanding`

Parser must handle:
- Multiple period types (instant vs. duration)
- Unit conversion (thousands vs. millions)
- Segment disaggregation (consolidated vs. division)
- Amendment supersession (use latest amendment as canonical)

### Step 4 — Airflow DAG (CV bullet 2)

`dags/edgar_pipeline.py` — one DAG, runs daily:

```
fetch_new_filings
    └── download_raw_documents
            └── parse_xbrl_facts
                    └── validate_quality_gates
                            └── load_to_warehouse
                                    └── update_audit_trail
                                            └── send_alerts_on_failure
```

Each stage writes to `pipeline_audit` on start and end.
`validate_quality_gates` raises `AirflowSkipException` if no new filings;
raises `AirflowFailException` if quality gates fail.

### Step 5 — Data quality monitoring (CV bullet 3)

`src/quality.py`:

**Completeness thresholds:**
```python
REQUIRED_FACTS = ["us-gaap:Revenues", "us-gaap:NetIncomeLoss", "us-gaap:Assets"]

def check_completeness(run_id: str, threshold: float = 0.95) -> QualityResult:
    """Fail if < threshold% of filings have all REQUIRED_FACTS populated."""
```

**PSI drift on numeric facts:**
```python
def compute_psi(baseline_values: list, current_values: list, bins: int = 10) -> float:
    """
    Population Stability Index.
    PSI < 0.1: no drift
    PSI 0.1–0.25: moderate drift, log warning
    PSI > 0.25: significant drift, trigger alert
    """
```

Run PSI on `fact_value` distribution for each `fact_name` comparing
current month vs. 3-month rolling baseline.

### Step 6 — Redis caching (CV bullet 3)

`src/cache.py` — cache hot lookups to avoid repeated EDGAR API calls:

```python
# Cache company filing index (TTL: 24h)
cache.set(f"filings:{cik}", json.dumps(filing_list), ex=86400)

# Cache parsed XBRL facts (TTL: 7d — facts don't change)
cache.set(f"facts:{accession_number}", json.dumps(facts), ex=604800)

# Cache CIK lookup by ticker (TTL: 1h)
cache.set(f"cik:{ticker}", cik, ex=3600)
```

### Step 7 — FastAPI serving layer (CV stack line)

`api/main.py` exposes parsed facts for downstream consumers:

```
GET  /health
GET  /filings/{ticker}           # list filings for a ticker
GET  /filing/{accession}         # get parsed facts for one filing
GET  /facts/{ticker}/{fact_name} # time-series of a specific fact
POST /trigger/{ticker}           # trigger on-demand ingestion
```

---

## Acceptance Criteria

| CV claim | Verifiable when |
|----------|-----------------|
| 10-K/10-Q ingestion to structured tables | `financial_facts` has rows for both form types |
| Accession-level lineage | `filing_versions` tracks amendments; `pipeline_audit` has stage logs |
| Immutable audit trails | `pipeline_audit` rows are append-only; no UPDATE/DELETE |
| Airflow ETL with stage-level validation gates | DAG runs end-to-end; `validate_quality_gates` task passes/fails gates |
| Rate-limited ingestion | `edgar_client.py` token bucket; no 429 errors in logs |
| PSI drift monitoring | `quality.py` computes PSI per fact; alerts at >0.25 |
| Redis caching | `cache.py` sets/gets CIK, filing index, XBRL facts |
| FastAPI serving layer | `GET /facts/{ticker}/{fact_name}` returns time-series JSON |

---

## Tech Stack (from CV skills)

- **Python:** Pandas, SQLAlchemy, requests (rate-limited), lxml (XBRL parse)
- **Data:** SEC EDGAR API, PostgreSQL (warehouse), Redis (cache)
- **Orchestration:** Airflow
- **Serving:** FastAPI
- **Quality:** PSI (custom), Great Expectations (optional)
- **Dev:** pytest (with mocked EDGAR responses), Docker Compose, GitHub Actions

---

## Important Distinction

This repo is the **data engineering layer** only:
- Ingestion, parsing, schema, quality, caching, serving
- No model training, no QLoRA, no vLLM, no fine-tuning

The old `Fine-Tuned-SEC-Filing-Extraction-Pipeline` repo contains the ML layer
(QLoRA Llama 3.1 fine-tuning for extraction) and is a separate project not
listed on the DE or Analyst CV.

---

## Priority Order

1. PostgreSQL schema + docker-compose (Postgres + Redis)
2. `src/edgar_client.py` with rate limiting
3. `src/xbrl_parser.py` for core financial facts
4. Airflow DAG skeleton with all stages and audit logging
5. `src/quality.py` — completeness + PSI
6. `src/cache.py` — Redis integration
7. `api/main.py` — FastAPI endpoints
8. Tests for client (mocked), parser, quality checks, DAG
