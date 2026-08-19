# API Contracts

The serving layer is defined in [`api/main.py`](../api/main.py). Every endpoint
declares a Pydantic `response_model`, so **the authoritative machine-readable
contract is the OpenAPI document FastAPI generates from the route definitions
themselves**:

```bash
make api                                  # uvicorn api.main:app --reload --port 8000
curl -s localhost:8000/openapi.json | jq  # full schema
open http://localhost:8000/docs           # interactive Swagger UI
```

This file is prose orientation for a reader. It deliberately does not restate
the field-by-field schemas — a hand-maintained copy would drift from the code,
and drift in an interface document is worse than no document.

---

## Endpoints

| Method | Path | Response model | Purpose |
|---|---|---|---|
| `GET` | `/health` | `HealthResponse` | Liveness, plus reachability of PostgreSQL and Redis |
| `GET` | `/filings/{ticker}` | `FilingsResponse` | Filings for a ticker, newest first |
| `GET` | `/filing/{accession}` | `FilingFactsResponse` | Every parsed fact for one filing |
| `GET` | `/facts/{ticker}/{fact_name}` | `TimeSeriesResponse` | One concept across time for one company |
| `GET` | `/anomalies/{ticker}` | `AnomaliesResponse` | Anomaly scores for that ticker's filings |
| `GET` | `/model/current` | `ModelInfoResponse` | Provenance of the promoted anomaly model |
| `GET` | `/audit/{accession}` | `AccessionAuditResponse` | Hash-chained extraction audit history |
| `POST` | `/trigger/{ticker}` | `TriggerResponse` | Request on-demand ingestion |

## Conventions

**Pagination.** List endpoints accept `limit` and `offset`. Responses carry the
returned window plus the total count, so a client can page without a second
call to discover the size.

**Caching.** Read endpoints consult Redis before PostgreSQL. TTLs follow how
fast the underlying data can legitimately change: CIK lookups 1 hour, filing
index 24 hours, parsed facts 7 days. A cache miss is never an error — it is a
slower correct answer.

**Errors.** A missing ticker or accession returns `404` with a `detail` string
naming what was not found. Malformed path or query parameters return `422` from
FastAPI's own validation, before any handler runs. Neither case returns a
partial or empty-but-successful body, so a client cannot mistake "not found" for
"found nothing."

**Identifiers.** Accession numbers are the SEC's dashed form
(`0000320193-24-000123`). Fact names are fully qualified XBRL concepts
(`us-gaap:Revenues`), not friendly aliases — the qualified name is what appears
in the filing and what the audit trail records.

## Provenance guarantees

Two endpoints exist specifically so that a consumer can verify rather than trust:

- **`/audit/{accession}`** returns the append-only extraction audit rows for a
  filing, each carrying `row_hash` and `prev_row_hash`. The chain is exactly
  what [`scripts/verify_audit_chain.py`](../scripts/verify_audit_chain.py)
  recomputes, so a client can independently confirm that no row was altered or
  removed after the fact.
- **`/model/current`** returns the promoted model's version, artifact SHA-256,
  training-data SHA-256, and git commit — enough to identify precisely which
  model produced any score served by `/anomalies/{ticker}`.

## What this API does not do

It does not serve raw filing documents, and it does not extract on demand
synchronously. `POST /trigger/{ticker}` requests ingestion and returns
immediately; extraction happens in the Airflow DAG, and the facts become
available through the read endpoints once that run completes.
