# SEC EDGAR Extraction Pipeline — Implementation Prompts

Use these prompts in sequence with Cursor Agent to build this project according to its CV description.

---

## Prompt 1 — Scaffold + schema + EDGAR client

This is a new repo. Read AGENTS.md for the full specification.

1. Create the directory structure from AGENTS.md.
2. Create docker-compose.yml with PostgreSQL and Redis services.
3. Build the full schema from AGENTS.md (filings_raw, financial_facts, filing_versions, pipeline_audit) using SQLAlchemy models in src/schema.py. Include Alembic for migrations.
4. Build src/edgar_client.py with the EdgarClient class from AGENTS.md:
   - get_company_filings(cik) → submission JSON from data.sec.gov
   - get_xbrl_facts(cik) → company facts JSON
   - get_filing_document(url) → raw HTML
   - search_filings(query, form_type, date_range) → list of accession numbers
   - Rate limit: 10 req/sec using a token bucket. User-Agent header required.
   - Retry on 429/503 with exponential backoff, max 5 retries.
5. Write tests/test_client.py mocking the SEC API responses with the requests-mock library. Test: rate limiting, retry on 503, correct parsing of submissions JSON.

Add requirements.txt: requests, sqlalchemy, alembic, psycopg2-binary, redis, lxml, fastapi, uvicorn, apache-airflow, pydantic, pytest, requests-mock.

---

## Prompt 2 — XBRL parser + Airflow DAG + data quality

Read AGENTS.md. EdgarClient and schema are working.

1. Build src/xbrl_parser.py: parse XBRL inline facts from SEC filing HTML. Extract the 7 target facts from AGENTS.md (Revenues, NetIncomeLoss, Assets, Liabilities, OperatingIncomeLoss, EarningsPerShareBasic, CommonStockSharesOutstanding). Handle: instant vs duration periods, thousands/millions unit conversion, segment disaggregation, amendment supersession. Return list of dicts matching financial_facts schema.

2. Build dags/edgar_pipeline.py: Airflow DAG with 6 tasks in order from AGENTS.md. Each task writes start/end rows to pipeline_audit. validate_quality_gates raises AirflowSkipException (no new filings) or AirflowFailException (quality failure). The DAG must run with MOCK_EDGAR=true using fixture responses.

3. Build src/quality.py with check_completeness() and compute_psi() from AGENTS.md. PSI thresholds: <0.1 clean, 0.1–0.25 warn, >0.25 alert.

4. Build src/cache.py: Redis caching for CIK lookups (TTL 1h), filing index (TTL 24h), XBRL facts (TTL 7d) using the key patterns in AGENTS.md.

5. Tests: test_parser.py (parse known XBRL fixture), test_quality.py (PSI edge cases), test_dag.py (DAG structure, task count, mock run).

---

## Prompt 3 — FastAPI serving layer

Read AGENTS.md. Pipeline, quality checks, and Redis cache are working.

Build api/main.py as a FastAPI app with all 5 endpoints from AGENTS.md:
  GET  /health
  GET  /filings/{ticker}           returns list of filings with metadata
  GET  /filing/{accession}         returns parsed financial_facts for one filing
  GET  /facts/{ticker}/{fact_name} returns time-series of a specific fact
  POST /trigger/{ticker}           triggers on-demand ingestion via Airflow API

All endpoints:
- Use Redis cache before hitting PostgreSQL
- Return consistent JSON schemas defined as Pydantic models
- Include pagination (limit/offset) on list endpoints
- Return 404 with detail message if ticker/accession not found

Add a simple OpenAPI description to each endpoint.
Run with: uvicorn api.main:app --reload
Write integration tests in tests/test_api.py using TestClient.
