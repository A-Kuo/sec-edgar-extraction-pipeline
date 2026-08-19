# Contributing to sec-edgar-extraction-pipeline

Thank you for your interest in contributing! This document outlines the process for contributing to this project.

## Ways to Contribute

- **Report bugs**: open an issue with a minimal reproduction and environment details
- **Suggest enhancements**: open an issue describing the feature and its use case
- **Submit code**: follow the workflow below for pull requests
- **Improve documentation**: fix typos, clarify explanations, add examples
- **Add test cases**: increase coverage, especially around dialect-specific SQL (PostgreSQL vs. SQLite) and DAG stage wiring

## Development Setup
# Contributing

This is primarily a personal/portfolio project, but it's built and tested like
one that takes outside contributions seriously. Issues and pull requests are
welcome.

## Development setup

```bash
git clone https://github.com/A-Kuo/sec-edgar-extraction-pipeline.git
cd sec-edgar-extraction-pipeline

pip install -r requirements-dev.txt
pre-commit install

make up          # Postgres + Redis
make migrate     # apply migrations
make test        # run the full suite — no network access needed, MOCK_EDGAR=true by default
```

## Code Style

- **Formatter**: `ruff format`
- **Linter**: `ruff check`
- **Type checker**: `mypy src api scripts` (`disallow_untyped_defs` is on — new functions need full type hints)
- **Migrations**: generated with `alembic revision --autogenerate`, never hand-edited after they're merged; SQLite-specific column/constraint changes need `op.batch_alter_table`

Run everything CI runs, in CI's order:

```bash
make ci
```

## Pull Request Workflow

1. **Branch** from `main` with a descriptive name
2. **Make changes** with clear, focused commits — see `docs/DECISION_LOG.md` for the level of "what was wrong, what changed, how it's verified" this repo's commit messages aim for
3. **Add tests** for new functionality, including the failure case a fix addresses, not just the happy path
4. **Run `make ci`** — lint, typecheck, tests, DAG import, migration reversibility
5. **Update documentation** if changing user-facing behavior — `README.md`, `AGENTS.md`, and any relevant file in `docs/`

### PR Checklist

- [ ] Tests pass (`make test`)
- [ ] Code is formatted (`make format`)
- [ ] No new lint errors (`make lint`)
- [ ] Type-checks (`make typecheck`)
- [ ] A new Alembic migration exists for any schema change, and `alembic check` reports no drift
- [ ] `README.md` / `AGENTS.md` updated if behavior visible to a user or the next contributor changed

## Project Structure Conventions

- `src/` — extraction, parsing, validation, persistence, alerting; `src/ml/` — anomaly detection (features, rules, model, registry, monitoring)
- `dags/` — the Airflow DAG orchestrating the pipeline end to end
- `api/` — the FastAPI serving layer
- `scripts/` — operational CLIs (backfill, validate, train/evaluate, audit-chain verification)
- `migrations/` — Alembic revisions
- `tests/` — one test module per source module; DAG and API tests run against real (SQLite or file-backed) databases rather than mocks wherever the behavior under test is database-specific
- `docs/` — design docs and the decision log; not user-facing setup instructions, which stay in `README.md`

## Testing Guidelines

- The full suite runs without live network access or a running Airflow scheduler — `MOCK_EDGAR=true` swaps in fixture data
- Prefer a real SQLite engine (in-memory or file-backed, per `tests/conftest.py` and existing fixtures) over mocking the database for anything that depends on actual SQL behavior — dialect-specific UPSERTs and the audit-trail hash chain are two places a mock would hide a real bug
- When adding a fix, prefer a test that would have failed under the old code over one that only confirms the new code works in isolation

## Commit Message Style

This repo's commits explain *why*, not just *what* — see `docs/DECISION_LOG.md` for the standard:

```
Replace blind INSERTs with idempotent UPSERTs

session.bulk_save_objects against financial_facts had no natural-key
constraint; a worker retry after a mid-batch crash duplicated every
fact in the batch. Replaced with INSERT ... ON CONFLICT keyed on a
content hash, dialect-dispatched for PostgreSQL/SQLite.
```

## Questions?

Open a discussion or an issue on GitHub, or check `AGENTS.md` for the architecture and design rationale this project already has written down.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
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
