# AGENTS.md — SEC EDGAR Extraction Pipeline

Architecture spec and build notes for contributors (human or AI) working on this repository. Extraction is deterministic (`lxml`, no LLM) — the ML layer *scores* already-extracted facts for anomalies, it never produces them.

## Overview

**Purpose:** Ingest SEC 10-K/10-Q filings from EDGAR, extract structured financial facts from XBRL, validate data quality, score filings for extraction anomalies, and serve the results through a cached API — all orchestrated by Airflow, with CI/CD gating every change including the model.

**Status:** Core pipeline (EDGAR client, XBRL parser, schema + migrations, quality checks, Redis cache, Airflow DAG, FastAPI layer) plus an anomaly-detection layer (`src/ml/`), a model registry, CI/CD workflows, and a Docker image are all in place. See [README.md](README.md) for setup and usage.

For *why* the system looks the way it does — real defects found and corrected, with commit hashes and measured before/after, not just the finished state — see [`docs/DECISION_LOG.md`](docs/DECISION_LOG.md).
**Status:** Core pipeline implemented — EDGAR client, XBRL parser, schema + migrations, quality checks, Redis cache, Airflow DAG, FastAPI layer, and test suite are all in place. A citation-grounded retrieval layer (`src/rag/`, served via `GET /ask/{ticker}`) sits on top of the structured pipeline as an optional research aid — see [README.md](README.md) for setup and usage.

## Repository Structure

```
sec-edgar-extraction-pipeline/
├── dags/
│   └── edgar_pipeline.py       # Airflow DAG (9 tasks, incl. score_anomalies)
├── src/
│   ├── edgar_client.py         # EDGAR API wrapper (rate-limited, retry)
│   ├── xbrl_parser.py          # XBRL/HTML fact extraction
│   ├── schema.py               # SQLAlchemy models (7 tables)
│   ├── metrics.py              # Per-run metrics aggregation (collect_run_metrics)
│   ├── quality.py              # completeness + PSI drift checks
│   ├── cache.py                # Redis caching layer
│   ├── alerts.py                # Slack/SMTP alerting hooks
│   └── ml/                      # anomaly detection
│       ├── features.py          # fact rows -> filing-level feature matrix
│       ├── rules.py             # deterministic plausibility checks
│       ├── model.py             # IsolationForest + rules, hybrid scoring
│       ├── registry.py          # content-addressed model versioning
│       ├── monitoring.py        # feature/prediction drift (reuses quality.py)
│       ├── evaluation.py        # corruption injection, the CI gate's metrics
│       └── synthetic.py         # seeded filing generator (CI has no DB)
├── api/
│   └── main.py                 # FastAPI serving layer (8 endpoints)
│   └── alerts.py               # Slack/SMTP alerting hooks
│   └── rag/
│       ├── chunker.py           # Filing text -> provenance-tagged chunks
│       ├── retrieval.py         # TF-IDF retrieval over chunks
│       ├── qa.py                # Citation-first answer construction, refusal behavior
│       └── evaluation.py        # Retrieval recall / citation validity / grounding-accuracy harness
├── api/
│   └── main.py                 # FastAPI serving layer (incl. GET /ask/{ticker})
├── scripts/
│   ├── backfill.py             # historical ingestion CLI
│   ├── validate.py             # manual quality-check CLI
│   ├── train_model.py          # train + register (+ optionally promote) a model
│   └── evaluate_model.py       # CI gate: floors + regression vs. promoted model
├── migrations/                  # Alembic environment + versions
├── tests/                       # pytest suite — see Testing Strategy below
├── .github/workflows/
│   ├── ci.yml                   # lint, mypy, tests (matrix), real DAG import, migrations
│   ├── ml.yml                   # train/evaluate/gate the model on PR
│   └── cd.yml                   # build + push image to GHCR on a version tag
├── Dockerfile                    # multi-stage; runs as non-root
├── docker-compose.yml            # Postgres + Redis (local)
├── Makefile                      # local commands mirroring CI
├── requirements.txt / requirements-dev.txt
├── pyproject.toml                # pytest, coverage, ruff, mypy config
└── AGENTS.md                     # this file
```

## Database Schema

Generated from `src/schema.py` via Alembic; see `migrations/versions/` for the exact DDL. Summary:

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
  id BIGSERIAL PRIMARY KEY,
  accession_number TEXT REFERENCES filings_raw(accession_number) ON DELETE CASCADE,
  fact_name TEXT NOT NULL,            -- e.g. us-gaap:Revenues
  fact_value NUMERIC,
  unit TEXT,
  period_start DATE,
  period_end DATE,
  segment TEXT,
  fact_hash CHAR(64) NOT NULL UNIQUE, -- sha256(accession|fact_name|period_start|period_end|segment)
  parsed_at TIMESTAMPTZ DEFAULT now()
);

-- Version history (immutable)
CREATE TABLE filing_versions (
  id BIGSERIAL PRIMARY KEY,
  cik TEXT NOT NULL,
  filing_type TEXT NOT NULL,
  period_of_report DATE NOT NULL,
  accession_number TEXT,
  is_amendment BOOLEAN DEFAULT false,
  superseded_by TEXT,                 -- accession_number of amendment
  recorded_at TIMESTAMPTZ DEFAULT now()
);

-- Stage-level audit trail (one row per DAG stage per run)
CREATE TABLE pipeline_audit (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  stage TEXT NOT NULL,                -- ingest | parse | validate | score | load
  status TEXT NOT NULL,               -- started | completed | failed
  records_processed INT,
  error_message TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Per-accession, hash-chained extraction audit trail (CMMC AU-2/AU-3/AU-12)
-- Append-only, enforced at the DB level: a BEFORE UPDATE OR DELETE trigger
-- (migration 2cf6396ba519, PostgreSQL only) rejects any mutation outright.
-- Each row's row_hash covers its own fields plus the prior row's row_hash
-- (src/audit.py::compute_row_hash), so tampering any historical row —
-- including bypassing the trigger with superuser DDL — is detectable by
-- recomputing the chain (scripts/verify_audit_chain.py). See
-- docs/AUDIT_TRAIL_PLAN.md for the full design and what a hash chain does
-- and does not prove.
CREATE TABLE extraction_audit (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  system_id TEXT NOT NULL,            -- which process/identity performed the extraction
  accession_number TEXT,              -- nullable: failures can predate a row in filings_raw
  stage TEXT NOT NULL,                -- download | parse | load
  extraction_status TEXT NOT NULL,    -- success | failure
  detail TEXT,
  content_hash TEXT,                  -- SHA-256 of the extracted content, when applicable
  prev_row_hash TEXT,                 -- NULL only for the chain's first row
  row_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- One row per anomaly-scoring pass — pins which model produced the scores
CREATE TABLE model_runs (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  model_version TEXT NOT NULL,
  model_sha256 TEXT NOT NULL,
  git_sha TEXT,
  filings_scored INT NOT NULL DEFAULT 0,
  anomalies_flagged INT NOT NULL DEFAULT 0,
  threshold FLOAT NOT NULL,
  drift_report JSONB,
  drift_level TEXT,                   -- clean | warn | alert
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (run_id, model_version)
);

-- Per-filing anomaly score, joined back to model_runs
CREATE TABLE fact_anomalies (
  id BIGSERIAL PRIMARY KEY,
  model_run_id BIGINT REFERENCES model_runs(id) ON DELETE CASCADE,
  accession_number TEXT REFERENCES filings_raw(accession_number) ON DELETE CASCADE,
  score FLOAT NOT NULL,
  model_score FLOAT NOT NULL DEFAULT 0,
  rule_score FLOAT NOT NULL DEFAULT 0,
  is_anomaly BOOLEAN NOT NULL DEFAULT false,
  triggered_by TEXT,                  -- rule | model | both
  reason TEXT,
  rule_violations JSONB,
  top_contributors JSONB,
  scored_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (model_run_id, accession_number)
);
```

Surrogate primary keys are declared `BigInteger().with_variant(Integer, "sqlite")` (see the comment on `src.schema.Base`) — plain `BigInteger` doesn't get SQLite's autoincrement rowid-alias behavior, so ORM-level inserts silently fail under the SQLite the test suite runs against, even though the same code works fine against PostgreSQL's `BIGSERIAL`.

## Component Notes

### `edgar_client.py`
- Submissions: `https://data.sec.gov/submissions/CIK{cik:010d}.json`
- XBRL facts: `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json`
- Full-text search: `https://efts.sec.gov/LATEST/search-index?q=...`
- SEC rate limit: 10 requests/second — enforced with a token-bucket limiter.
- A `User-Agent` header is required by SEC; requests without one return 403.
- Retries on 429/503 (and on request exceptions), max 5 attempts, with **full-jitter exponential backoff** (`full_jitter_backoff()`): `sleep = uniform(0, min(cap, base * 2**attempt))`. A deterministic backoff curve — sleep for exactly `backoff`, then double — makes every worker that hits the same rate limit retry in lockstep, which is itself a self-inflicted ban risk against a shared endpoint; drawing the actual sleep from a uniform distribution spreads retries across the window instead. A server-supplied `Retry-After` is still honored as a floor, with a few seconds of jitter added on top so identical `Retry-After` values don't resynchronize workers into the next wave.

### `xbrl_parser.py`
Extracts a fixed set of target facts: `us-gaap:Revenues` (or `RevenueFromContractWithCustomerExcludingAssessedTax`), `NetIncomeLoss`, `Assets`, `Liabilities`, `OperatingIncomeLoss`, `EarningsPerShareBasic`, `CommonStockSharesOutstanding`. Handles instant vs. duration periods, unit conversion (thousands vs. millions), segment disaggregation, and amendment supersession (latest amendment wins).

### `dags/edgar_pipeline.py`
Eight-task linear DAG:

```
fetch_new_filings → download_raw_documents → parse_xbrl_facts
  → validate_quality_gates → score_anomalies → load_to_warehouse
  → update_audit_trail → send_alerts_on_failure
```

Each stage writes a start/end row to `pipeline_audit`. `validate_quality_gates` raises `AirflowSkipException` when there are no new filings, or `AirflowFailException` when quality gates fail. `score_anomalies` is deliberately **non-blocking**: a missing or unverifiable model logs a warning and returns zero scores rather than failing the run — anomaly scoring is a review aid, not a gate, and an ML outage must never block filings that already passed the deterministic quality checks.

Airflow-version compatibility: the module imports from both the Airflow 2.x and 3.x module layout via `try`/`except ImportError`, since Airflow 3 relocated `PythonOperator`, `TriggerRule`, and the exceptions, and renamed the DAG's `schedule_interval` argument to `schedule`. `tests/test_dag_import.py` imports the module in a subprocess against whatever Airflow is actually installed, specifically to catch a future relocation before it reaches production — see that file's docstring for why `tests/test_dag.py`'s mocked-Airflow tests couldn't catch the version that shipped originally.

### `quality.py`
- **Completeness:** fails a run if fewer than a configurable threshold (default 95%) of filings have all required facts populated.
- **PSI drift:** Population Stability Index on `fact_value` distributions, current run vs. a rolling baseline. `<0.1` clean, `0.1–0.25` warn, `>0.25` alert. `compute_psi()` supports both `strategy="uniform"` (default, equal-width bins) and `strategy="quantile"` (equal-frequency bins from the baseline) — quantile binning avoids the near-empty-bin epsilon artifact that equal-width bins produce on skewed distributions (financial magnitudes are always skewed). `src/ml/monitoring.py` uses quantile binning for exactly this reason.

### `cache.py`
Redis key patterns: `cik:{ticker}` (TTL 1h), `filings:{cik}` (TTL 24h), `facts:{accession_number}` (TTL 7d — parsed facts don't change).

### `src/upsert.py` — idempotent writes

Every DB write the DAG makes goes through this module as a single atomic `INSERT ... ON CONFLICT` statement, not a blind `INSERT` or a check-then-insert. This exists because `_bulk_insert_facts` used to call `session.bulk_save_objects(objects)` against a table with no natural-key constraint — an Airflow retry after a worker died mid-batch (`default_args["retries"] = 2`) would duplicate every fact already committed.

- `financial_facts` — keyed on `fact_hash`, a SHA-256 of the fact's natural key (`compute_fact_hash()`: accession, concept, period, segment). A retry recomputes the identical hash and updates the row in place.
- `filings_raw` — keyed on the existing `accession_number` PK, `DO NOTHING` on conflict: a retry re-landing an already-present accession is a no-op, not a silent overwrite.
- `model_runs` — keyed on `(run_id, model_version)`, `DO UPDATE`. Replaced a delete-then-recreate pattern; the `id` (and therefore every `fact_anomalies` row referencing it) now stays stable across a retry instead of churning.
- `fact_anomalies` — keyed on `(model_run_id, accession_number)`, matching `uq_anomaly_run_accession`.

`_insert()` dispatches the dialect-specific `insert()` constructor (`sqlalchemy.dialects.postgresql` / `.sqlite`) by the bound engine's dialect name. Both expose an identical `.on_conflict_do_update()` / `.on_conflict_do_nothing()` API, which is what lets `tests/test_upsert.py` exercise the *real* conflict-resolution SQL against SQLite rather than mocking it — production runs the same Python against PostgreSQL, only the imported dialect module differs. `TestWorkerRestartMidBatch` in that file directly reproduces the failure mode: it replays a full and a partial batch after a simulated crash and asserts no duplicate rows result.

The `financial_facts.fact_hash` and `model_runs (run_id, model_version)` constraints were added in migration `03255b5bee46`, which backfills `fact_hash` for any pre-existing rows using the real `compute_fact_hash()` (imported, not reimplemented in SQL) and defensively de-duplicates `model_runs` before adding its constraint.

### `src/ml/` — anomaly detection

**Why a model at all:** extraction stays deterministic; nothing here changes a reported number. The model flags filings whose *extracted* facts look internally inconsistent — a scale error, a sign flip, a dropped required fact, an EPS that doesn't reconcile with net income and shares — producing a ranked review queue instead of leaving that discovery to a manual audit.

**Hybrid scoring, not just IsolationForest.** A forest alone measured 0.17 recall on dropped required facts: every training filing has full fact coverage, so that feature has zero training variance, `StandardScaler` flattens it, and the signal is gone before any tree sees it. `src/ml/rules.py` asserts the invariants a well-formed filing must satisfy (assets > 0, EPS × shares ≈ net income, leverage ∈ [0, 1.5], …); a filing's score is `max(model_score, rule_score)`, and each score records `triggered_by` (`rule` / `model` / `both`) plus the specific rule violations, so a reviewer sees *why* a filing was flagged, not just a number. This lifted recall from 0.43 → 0.73 and precision from 0.30 → 0.55 (measured via `evaluate_model.py` against injected corruptions).

**Calibration matters as much as the model.** A naive min-max rescale of the IsolationForest decision function put `threshold=0.6` at the training set's 90th percentile, so a detector configured for 5% contamination flagged 10% of its own training data — every one of those a false positive. `AnomalyDetector._normalise()` instead pins the training set's `contamination`-quantile decision value to exactly `threshold`, so the configured contamination is the actual flag rate on in-distribution data.

**Registry (`registry.py`):** every registered version records the artifact's SHA-256, the training data's SHA-256 (`hash_training_data()` — accessions + feature names + rounded values), the git commit, library versions, and evaluation metrics. `ModelRegistry.verify()` re-hashes the artifact on disk and compares it against the recorded digest — a model swapped after registration fails to load rather than silently scoring production traffic. `promote()` calls `verify()` before moving the `PRODUCTION` pointer. Each registration also writes a `MODEL_CARD.md` (intended use / out of scope / limitations / reproduction steps).

**Monitoring (`monitoring.py`):** reuses `src.quality.compute_psi` rather than a second drift metric, so a feature-drift alert and a fact-drift alert read on the same scale. Known limitation, documented in the module: filings cluster by company and scale features follow a per-company random walk, so per-feature PSI on a small comparison batch (tens of companies) is noisy — `prediction_level` (PSI on the score distribution) is the stable signal to alert on; see the module docstring for the numbers behind that claim.

**Evaluation (`evaluation.py`):** there is no labelled corpus of mis-extracted filings, so `inject_corruptions()` manufactures the labels — scale errors, sign flips, dropped facts, EPS mismatches — against held-out filings, and `evaluate_detector()` reports recall/precision/F1/ROC-AUC at the model's configured threshold. This is what `scripts/evaluate_model.py` gates a merge on.

**Synthetic corpus (`synthetic.py`):** a seeded generator producing internally-consistent filings across five sector archetypes, so the entire train → evaluate → gate path runs from a clean checkout with no database and no SEC access — this is what CI trains and gates against.

### `scripts/train_model.py` / `scripts/evaluate_model.py`
Training is fully specified by explicit flags (`--seed`, `--contamination`, `--n-estimators`, `--data-seed`) with fixed defaults, so identical flags on identical data reproduce an identical artifact hash — verified in `.github/workflows/ml.yml` by training twice and comparing. `evaluate_model.py` checks absolute floors (recall/precision/ROC-AUC/max flag-rate) and, when a `PRODUCTION` model exists, a regression check against it with a small tolerance for measurement noise; exit code 0 = pass, 1 = failed a floor or regressed, 2 = could not evaluate (missing file, empty registry).
### `src/rag/` — retrieval layer

Chunks filing text into Item-level sections (`chunker.py`), retrieves the most relevant chunks for a question via TF-IDF (`retrieval.py`), and constructs a citation-first answer (`qa.py`). Deliberately **not** an embedding-based system: the target corpus size (a scoped watchlist, not all of EDGAR) doesn't need one, and TF-IDF keeps retrieval deterministic, offline, and fast to test.

Key behaviors, and why they exist:

- **Refusal, not confident guessing.** `answer_question()` returns `grounded=False` and a fixed refusal string when the best retrieval score is below `MIN_GROUNDING_SCORE` (0.15). This threshold was tuned empirically against the narrative fixtures in `tests/fixtures/sample_filing_narrative_10k.html` / `_10q.html` — see the test history in `tests/test_rag.py` for the calibration process, including a real bug it caught (see below).
- **Domain-aware stopwords.** SEC boilerplate ("fiscal", "the Company", "this Item") appears in nearly every chunk of nearly every filing and would otherwise dominate cosine similarity for out-of-scope questions that happen to mention a fiscal year. `_FILING_BOILERPLATE_STOPWORDS` in `retrieval.py` strips these before vectorizing.
- **Sentence-level citation windows, not chunk-start truncation.** A naive `text[:280]` snippet shows the section heading and opening boilerplate, not the sentence that actually supports the answer. `_best_sentence_window()` in `qa.py` picks the sentence with the highest term overlap with the question and starts the snippet there.
- **Optional LLM synthesis, never required.** If `OPENAI_API_KEY` is set, `_try_llm_synthesis()` asks a model to phrase the answer using *only* the retrieved chunk text. Any failure (missing key, network error, bad response) falls back to the extractive path silently — this keeps `/ask` and its tests fully offline by default.

**Known limitation — entity disambiguation.** A lexical retriever cannot reliably tell "this is about a different company" from "this happens to share financial boilerplate vocabulary." Early testing found "What was Tesla's revenue in fiscal 2024?" scoring *above* the grounding threshold against an Apple-only corpus, purely on the strength of the shared phrase "fiscal 2024." This is mitigated at the API layer: `GET /ask/{ticker}` only ever indexes filings for the requested ticker, so cross-entity confusion mostly can't occur in practice — but the retriever module in isolation (as exercised directly in `tests/test_rag.py`) doesn't solve this itself, and its evaluation cases were revised to test genuine topic-absence (e.g. "return policy for damaged retail products") rather than cross-entity mixups, which are a different, harder problem (would need NER/entity resolution or semantic embeddings, not lexical matching).

**Known limitation — `raw_html` is currently the filing index page, not the primary document.** `download_raw_documents` in `dags/edgar_pipeline.py` fetches the accession's directory-listing HTML, not the actual 10-K/10-Q document body. `/ask` and the RAG tests are validated against realistic narrative fixtures, but will need this ingestion gap closed before returning useful answers against real production data. Fixing this is a data-engineering task (locate and fetch the primary document filename from the index, not the index itself), independent of the retrieval layer.

### `api/main.py`
```
GET  /health
GET  /filings/{ticker}            list filings for a ticker (paginated)
GET  /filing/{accession}          parsed facts for one filing
GET  /facts/{ticker}/{fact_name}  time-series of a specific fact
GET  /anomalies/{ticker}          anomaly scores + reasons (not cached)
GET  /model/current               provenance of the promoted model (not cached)
POST /trigger/{ticker}            trigger on-demand ingestion
```
Filing and fact reads check Redis before PostgreSQL. `/anomalies` and `/model/current` deliberately bypass the cache — scores change whenever a new model is promoted, and a stale score is worse than a slow one. Unknown ticker/accession returns 404 with a detail message.

## CI/CD

- **`ci.yml`** — ruff (lint + format), mypy (`src api scripts`), pytest matrix (3.11/3.12) with a 75% coverage gate, and a dedicated job that applies migrations to a real PostgreSQL service container, asserts every table exists, checks the migrations are reversible (`alembic downgrade base && upgrade head`), runs `alembic check` to catch model/migration drift, and imports the DAG in a subprocess against the real installed Airflow — see `tests/test_dag_import.py`.
- **`ml.yml`** — trains a candidate on the synthetic corpus, asserts training is reproducible (trains twice, compares artifact hashes), gates it with `evaluate_model.py`, verifies the artifact hash, and runs a two-sided drift-monitor sanity check (quiet on an equivalent population, alerts on a 1000x systematic revenue shift). Triggers on changes to `src/ml/**`, `src/quality.py`, or the training/eval scripts.
- **`cd.yml`** — on a `v*.*.*` tag: re-runs lint/tests/DAG-import as a release gate, builds a multi-stage Docker image, smoke-tests it (imports the application inside the built container), generates an SBOM, scans for vulnerabilities (reported, not blocking — a HIGH/CRITICAL base-image CVE with no available fix shouldn't block every release), and pushes to GHCR with build provenance attestation.
GET  /ask/{ticker}                citation-grounded Q&A over the ticker's indexed filing text
POST /trigger/{ticker}             trigger on-demand ingestion
```
All read endpoints check Redis before PostgreSQL and return typed Pydantic models. Unknown ticker/accession returns 404 with a detail message. `/ask` builds its retrieval index on demand from `filings_raw.raw_html` per request rather than caching it — acceptable at the corpus sizes this project targets; worth revisiting (e.g. cache the fitted retriever in Redis, invalidate on new filings) if ticker filing counts grow large enough for TF-IDF fitting to become a latency concern.

## Testing Strategy

- **Unit:** mocked SEC API responses (`requests-mock`), fixture XBRL HTML for the parser, PSI/completeness edge cases, ML feature/rule/model/registry/monitoring tests against a small synthetic corpus.
- **Integration:** DAG structure/wiring tests (mocked Airflow, `tests/test_dag.py`), a *separate* real-Airflow import test (`tests/test_dag_import.py`), `TestClient`-based API tests against an in-memory SQLite DB and mocked Redis, ORM smoke tests (`tests/test_schema.py`) that instantiate every model against real SQLite rather than only ever hitting it through raw `text()` queries.
- Set `MOCK_EDGAR=true` to run the full suite without live network calls (the session-scoped `airflow_test_env` fixture in `conftest.py` sets this, plus a file-backed — not `:memory:` — SQLite URL for Airflow's own metastore, since Airflow 3 rejects a relative in-memory path outright).
- `make ci` runs the same lint → typecheck → test → dag-check sequence CI runs, in the same order.

## Common Pitfalls

| Issue | Solution |
|---|---|
| 403 from SEC EDGAR | Missing or malformed `User-Agent` header — must identify the app and a contact email |
| 429/503 from SEC EDGAR | Token-bucket limiter or backoff misconfigured — verify `RATE_LIMIT_RPS` and retry count |
| Duplicate facts across amendments | Ensure `filing_versions.superseded_by` is respected when querying "current" facts |
| Cache staleness | Facts TTL is 7 days — invalidate manually via `cache.py` if a filing is corrected out-of-band |
| DAG fails to import against a newer Airflow | This has happened once already (an unbounded `apache-airflow>=2.8.0` resolved to 3.x and broke `schedule_interval`). Run `pytest tests/test_dag_import.py -v` — it imports against whatever Airflow is actually installed, not a mock |
| `alembic upgrade head` creates nothing | Check `migrations/versions/` isn't back down to just a `.gitkeep` — this happened once when the directory existed but no revision had been committed |
| ORM insert fails on SQLite with a NOT NULL error on `id` | The primary key needs `BigInteger().with_variant(Integer, "sqlite")`, not plain `BigInteger` — see the comment on `src.schema.Base` |
| Per-feature drift alert on an equivalent population | Expected at small comparison-batch sizes (see "Known limitation" in `src/ml/monitoring.py`); check `prediction_level` instead, or compare batches of comparable size |
| `evaluate_model.py` exits 2 | "Could not evaluate" — missing metrics file, or no models registered when using `--version`. Distinct from exit 1 ("failed the gate") |
| A DB write duplicates rows on Airflow retry | Should not happen — every write goes through `src/upsert.py`'s `ON CONFLICT` helpers. If you're adding a new write path, use `src/upsert.py`, not `session.add()` / `bulk_save_objects()` directly, or you will reintroduce the bug that module exists to close |
| Adding a new UPSERT target | The conflict column(s) need a real UNIQUE constraint in `src/schema.py` *and* a migration — `ON CONFLICT` silently becomes a no-op-free plain INSERT without one on some backends, or an outright error on others. Test against SQLite via `sqlalchemy.dialects.sqlite.insert()` first (see `tests/test_upsert.py`) — it enforces real conflict semantics without needing a live Postgres container |

## Agent Handoff Checklist

When handing this project to another contributor or agent, update this section:

**Last updated:** August 2026
**Current state:** This repo is being wound down. Its `main` branch (core
pipeline, RAG retrieval, analytics marts) was merged into
[`A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline`](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline)
as that repo's `warehouse/` subsystem — see [`MIGRATION.md`](MIGRATION.md)
for the full path/env-var/port mapping and known differences. This branch
(`claude/sec-edgar-extraction-pipeline-ilcxmv`) is a separate, still-open line
of work — [PR #1](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/pull/1) —
that forked from `main` before that merge and added the ML anomaly-detection
layer, model registry, idempotent DB writes (`src/upsert.py`), jittered retry
backoff, the hash-chained per-accession audit trail (`src/audit.py`,
`extraction_audit`), metrics/benchmarking, and CI/CD. All of it is implemented
and passing (501 tests) but **none of it has moved to the destination repo** —
PR #1 is unmerged and currently conflicts with `main` (see `MIGRATION.md`'s
"Work that has not moved" section). Do not treat this repo as a place for new
feature work; the items below are tracked here only until they're moved.

**Next steps — now tracked in the destination repo, not here:**
- Resolve [PR #1](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/pull/1)
  (this branch) into `main`, then decide whether/how to port its audit-trail,
  ML anomaly-detection, and metrics work into the destination repo's
  `warehouse/` subsystem — this is the one item that has to happen *before*
  this repo can be archived, since it's the only place that work exists
- Production deployment guide beyond the container image (no Kubernetes/Helm config exists yet)
- HashiCorp Vault / cloud secrets manager integration (currently plain environment variables) — also the real `system_id` source `src/audit.py::current_system_id()` should eventually read from instead of the `SYSTEM_ID` environment variable it falls back to today
- A labelled (not just synthetic-corruption) evaluation set for the anomaly detector, once enough real flagged filings have been reviewed to build one
- Whether `pipeline_audit` and `extraction_audit` should eventually merge into one table (see `docs/AUDIT_TRAIL_PLAN.md` §11) — left as two tables at different granularities for now, revisit only if it becomes a real pain point

If you have write access to `A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline`,
consider opening these as issues there instead of leaving them as prose here.

## Related Documents

- [`docs/DECISION_LOG.md`](docs/DECISION_LOG.md) — real engineering decisions with evidence: defects found and corrected (commit hashes, before/after metrics, the test that proves each fix), and the architecture choices made deliberately from the outset.
- [`docs/AUDIT_TRAIL_PLAN.md`](docs/AUDIT_TRAIL_PLAN.md) — the design rationale behind the implemented hash-chained audit trail: schema, hash-chain construction, PostgreSQL trigger, and what a hash chain does and doesn't prove (§9 of the decision log has what shipped and what changed from this plan during implementation).
- [`docs/MICROSERVICE_ALTERNATIVE.md`](docs/MICROSERVICE_ALTERNATIVE.md) — a labeled, never-built comparative design: what a microservice decomposition of this system would look like, and why the monolith was chosen instead, argued from this project's own documented defects rather than general architecture-blog wisdom.
**Current state:** Core pipeline complete — ingestion, parsing, schema/migrations, quality checks, cache, DAG, API, and tests are all implemented and passing. Citation-grounded retrieval layer (`src/rag/`, `GET /ask/{ticker}`) added with an evaluation harness (`tests/test_rag.py::TestEvaluationHarness`) covering retrieval recall@k, citation validity, and grounding-decision accuracy on a 10-case seed set.

**Next steps (not yet done):**
- Fix `download_raw_documents` to persist the primary filing document, not the index page (blocks `/ask` from being useful against real ingested data — see "Known limitation" under `src/rag/` above)
- Grow the RAG evaluation set from 10 seed cases toward the 25-50 recommended for a credible eval, and add cases that stress cross-document/cross-period comparative questions
- CI workflow (lint/type-check/test on push)
- `.env.example` for local setup
- Production deployment guide (current `docker-compose.yml` is local-dev only; no Kubernetes/Helm config exists yet)
- There is separate, uncommitted work in progress on load-stage idempotency (`_upsert_facts` in `dags/edgar_pipeline.py`, a unique expression index in `src/schema.py`, plus `tests/test_load_idempotency.py`) — unrelated to the RAG layer, not touched by it, and not yet committed as of this update

## Resources

- SEC EDGAR API docs: https://www.sec.gov/edgar/sec-api-documentation
- Airflow docs: https://airflow.apache.org/docs/
- FastAPI docs: https://fastapi.tiangolo.com/
- Model Cards (Mitchell et al., 2019): https://arxiv.org/abs/1810.03993
