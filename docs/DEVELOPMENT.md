# Development Guide

## Project Overview

The SEC EDGAR Extraction Pipeline is a production-grade data engineering system for:
1. Fetching SEC 10-K/10-Q filings via EDGAR API
2. Parsing XBRL facts (8 financial concepts)
3. Validating data quality (completeness + PSI drift)
4. Storing in PostgreSQL with append-only audit trail
5. Serving via FastAPI with Redis caching

**Key constraint:** Deterministic XBRL parsing (no LLM/ML).

## Code Organization

```
src/
  ├── schema.py        # SQLAlchemy ORM models (4 tables)
  ├── edgar_client.py  # SEC EDGAR API client (rate-limited)
  ├── xbrl_parser.py   # XBRL → financial facts
  ├── quality.py       # PSI drift + completeness checks
  ├── cache.py         # Redis caching layer
  └── alerts.py        # Slack/SMTP notifications

api/
  └── main.py          # FastAPI serving layer (5 endpoints)

dags/
  └── edgar_pipeline.py # Airflow DAG (7 tasks)

scripts/
  ├── backfill.py      # Historical data ingestion CLI
  └── validate.py      # Quality check runner CLI

tests/
  ├── conftest.py      # pytest fixtures
  ├── test_api.py      # API tests (14 tests)
  ├── test_client.py   # Client tests (8 tests)
  ├── test_parser.py   # Parser tests (18 tests)
  ├── test_quality.py  # Quality tests (9 tests)
  └── test_dag.py      # DAG tests (7 tests)
```

## Key Components

### 1. Schema (`src/schema.py`)

Four SQLAlchemy 2.0 ORM models:

**FilingRaw** — Raw landing zone
```python
class FilingRaw(Base):
    accession_number: str (PK)
    cik: str (indexed)
    ticker: str (indexed)
    filing_date: datetime (indexed)
    period_end: datetime
    raw_html: str
    pipeline_run_id: str (indexed)
```

**FinancialFact** — Parsed XBRL facts
```python
class FinancialFact(Base):
    accession_number: str (FK → FilingRaw)
    fact_name: str (indexed, e.g., "Revenues")
    unit: str (e.g., "USD")
    period_end: datetime (indexed)
    value: float
    segment: str (e.g., "Total")
```

**FilingVersion** — Amendment tracking
```python
class FilingVersion(Base):
    original_accession: str (FK)
    amendment_accession: str
    superseded_by: str
```

**PipelineAudit** — Append-only audit trail
```python
class PipelineAudit(Base):
    run_id: str (indexed)
    stage: str (e.g., "fetch_new_filings")
    status: str ("started", "completed", "failed")
    row_count: int
    error_message: str (nullable)
    created_at: datetime (indexed)
```

### 2. EDGAR Client (`src/edgar_client.py`)

Token bucket rate limiter ensures ≤10 req/s to SEC:

```python
client = EdgarClient(user_agent="SEC-EDGAR-Pipeline your@email.com")
filings = client.get_company_filings("0000320193", "10-K")
```

**Features:**
- Thread-safe token bucket (10 req/s)
- Automatic retry on 429/503 (exponential backoff, max 5 retries)
- Honors `Retry-After` header
- User-Agent header required by SEC (403 without it)

### 3. XBRL Parser (`src/xbrl_parser.py`)

Extracts 8 financial concepts from iXBRL HTML:

```python
parser = XBRLParser()
facts = parser.parse(html_string, accession_number)
# Returns: List[FinancialFactRow]
```

**8 target concepts:**
1. Revenues
2. RevenueFromContract (alias for Revenues)
3. NetIncomeLoss
4. Assets
5. Liabilities
6. OperatingIncomeLoss
7. EarningsPerShareBasic
8. CommonStockSharesOutstanding

**Handles:**
- Duration vs instant contexts (different date pairs)
- Scale attribute (millions, thousands)
- Sign attribute (negatives in parentheses)
- Segment disaggregation
- XML namespace recovery

### 4. Quality Checks (`src/quality.py`)

**PSI (Population Stability Index):**
```python
psi = compute_psi(baseline=[values], current=[values])
level = classify_psi(psi)  # CLEAN, WARN, or ALERT
# CLEAN: <0.10, WARN: 0.10–0.25, ALERT: >0.25
```

**Completeness:**
```python
result = check_completeness(run_id, facts_by_accession, threshold=0.95)
# Raises ValueError if <95% facts present
```

### 5. Redis Cache (`src/cache.py`)

Three key patterns with configurable TTLs:

```python
cache = FilingCache("redis://localhost:6379/0")

# CIK → ticker mapping (1 hour)
cache.set_cik_ticker("AAPL", "0000320193")

# All filings for a CIK (24 hours)
cache.set_filings("0000320193", filings_list)

# All facts for a filing (7 days)
cache.set_facts("0000320193-23-000077", facts_list)
```

**Graceful degradation:** If Redis unavailable, API queries database directly (slower but functional).

### 6. FastAPI Serving (`api/main.py`)

5 endpoints with cache-first strategy:

**GET /health**
```bash
curl http://localhost:8000/health
# {"status": "ok", "version": "1.0.0"}
```

**GET /filings/{ticker}**
```bash
curl "http://localhost:8000/filings/AAPL?limit=10&offset=0"
# Returns paginated list with metadata
```

**GET /filing/{accession}**
```bash
curl http://localhost:8000/filing/0000320193-23-000077
# Returns all parsed facts for filing
```

**GET /facts/{ticker}/{fact_name}**
```bash
curl http://localhost:8000/facts/AAPL/Revenues
# Returns time-series across all filings
```

**POST /trigger/{ticker}**
```bash
curl -X POST http://localhost:8000/trigger/AAPL
# Returns {run_id, status: "queued"}
```

### 7. Airflow DAG (`dags/edgar_pipeline.py`)

7-task linear pipeline runs daily:

```
fetch_new_filings
      ↓
download_raw_documents
      ↓
parse_xbrl_facts
      ↓
validate_quality_gates (raises AirflowSkipException on zero filings)
      ↓
load_to_warehouse
      ↓
update_audit_trail
      ↓
send_alerts_on_failure (trigger_rule: one_failed)
```

Each task logs to `pipeline_audit` table (append-only, never UPDATE/DELETE).

## Common Development Tasks

### Adding a new financial concept

1. **Update XBRL parser:**
   ```python
   # src/xbrl_parser.py
   TARGET_CONCEPTS = {
       "Revenues",
       "MyNewConcept",  # Add here
   }
   ```

2. **Add test case:**
   ```python
   # tests/test_parser.py
   def test_parse_my_new_concept(self):
       facts = parser.parse(sample_ixbrl_html, "accession")
       assert any(f.fact_name == "MyNewConcept" for f in facts)
   ```

3. **Run tests:**
   ```bash
   pytest tests/test_parser.py -v
   ```

### Adding a new API endpoint

1. **Define schema:**
   ```python
   # api/main.py
   class MyResponse(BaseModel):
       data: str
   ```

2. **Implement endpoint:**
   ```python
   @app.get("/my-endpoint", response_model=MyResponse)
   async def my_endpoint(db: Session = Depends(get_db)):
       data = db.query(SomeModel).first()
       return MyResponse(data=data.value)
   ```

3. **Add test:**
   ```python
   # tests/test_api.py
   def test_my_endpoint(self, client):
       response = client.get("/my-endpoint")
       assert response.status_code == 200
   ```

### Running Airflow locally

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow dags list
airflow dags test edgar_pipeline 2024-01-15
```

### Debugging a test

```bash
# Run with verbose output and print statements visible
pytest tests/test_parser.py::TestXBRLParser::test_specific -vvs

# Run with pdb on failure
pytest tests/test_parser.py -vvs --pdb

# Run single test
pytest tests/test_api.py::TestHealthEndpoint::test_health_check_success -v
```

## Database Migrations

Using Alembic to version schema changes:

```bash
# Create migration (auto-detect changes)
alembic revision --autogenerate -m "Add new column"

# Review generated migration in migrations/versions/

# Apply migration
alembic upgrade head

# Rollback
alembic downgrade -1

# Check current revision
alembic current
```

**Important:** Migrations are run automatically by CI/CD and in docker-compose startup.

## Performance Profiling

### API latency

```bash
# Run with timing info
time curl http://localhost:8000/facts/AAPL/Revenues

# Use Apache Bench for load testing
ab -n 100 -c 10 http://localhost:8000/health
```

### Database query profiling

```python
# In api/main.py, enable query logging
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
```

### Cache hit rate

```python
# Redis CLI to monitor
redis-cli MONITOR
```

## Troubleshooting

### Tests fail with "ModuleNotFoundError"

```bash
pip install -e ".[dev]"
```

### Docker services not healthy

```bash
docker-compose ps  # Check status
docker-compose logs postgres  # View logs
docker-compose restart postgres  # Restart
```

### CI failing but tests pass locally

Check:
```bash
make lint      # Ruff formatting
make type-check  # mypy type errors
make test      # Coverage thresholds
```

### Database migrations out of sync

```bash
make db-reset  # Drop and recreate database
make migrate   # Apply all migrations
```

## Production Deployment

See [Deployment Guide](./DEPLOYMENT.md) for Kubernetes/Docker Swarm setup.

Key considerations:
- Connection pooling (sqlalchemy pool_pre_ping)
- Redis cluster (not single instance)
- Airflow worker scaling
- PostgreSQL backup strategy
- SSL certificates for API

## Performance Targets

| Metric | Target | Achieved |
|--------|--------|----------|
| API latency (p50) | <200ms | ~50ms (cached) |
| API latency (p99) | <1s | ~500ms (DB hit) |
| Cache hit rate | >80% | ~85% in production |
| Test suite | <5s | ~1.2s |
| DAG execution | <10m | ~8m for 50 filings |

---

For questions, see [CONTRIBUTING.md](./CONTRIBUTING.md) or email aus.kuo03@gmail.com.
