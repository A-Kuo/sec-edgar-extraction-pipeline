# Contributing

This is primarily a personal/portfolio project, but it's built and tested like
one that takes outside contributions seriously. Issues and pull requests are
welcome.

## Development setup

```bash
git clone https://github.com/A-Kuo/sec-edgar-extraction-pipeline.git
cd sec-edgar-extraction-pipeline
pip install -r requirements.txt

export MOCK_EDGAR=true   # use fixture data instead of live SEC calls
```

No live network access, Postgres, or Redis is required to develop or run the
test suite — `MOCK_EDGAR=true` and an in-memory SQLite database cover the
whole pipeline. See [README.md](README.md#quick-start) if you want to run
against real Postgres/Redis via `docker-compose up -d`.

## Before opening a pull request

```bash
pytest tests/ -v          # full suite must pass
```

There is no CI configured on this repository yet, so a clean local test run
is the bar for now. If you're touching `src/schema.py`, also generate and
apply an Alembic migration:

```bash
alembic revision --autogenerate -m "describe the schema change"
alembic upgrade head
```

## Guidelines

- **Keep extraction deterministic.** No LLM calls belong in the ingest →
  parse → validate → load path (`dags/edgar_pipeline.py`, `src/xbrl_parser.py`,
  `src/quality.py`). The retrieval layer (`src/rag/`) is the one place an LLM
  may optionally participate, and only to phrase an answer already
  constrained to retrieved text — see [Key Design Decisions](README.md#key-design-decisions).
- **New behavior needs a test.** `tests/` mirrors the module layout
  (`test_client.py`, `test_parser.py`, `test_quality.py`, `test_dag.py`,
  `test_api.py`, `test_rag.py`, `test_load_idempotency.py`). Add to the
  matching file rather than creating a new one unless you're adding a new
  module.
- **Idempotency matters.** Anything that writes to `financial_facts` or
  `filings_raw` should be safe to run twice (Airflow retries tasks; backfills
  get re-run). See `tests/test_load_idempotency.py` for the pattern.
- **Small, reviewable commits.** One logical change per commit, with a
  message explaining *why*, not just what changed.

## Reporting bugs

Open a [GitHub issue](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/issues)
with the command you ran, what you expected, and what happened. If it's
data-related, include the accession number or ticker involved.
