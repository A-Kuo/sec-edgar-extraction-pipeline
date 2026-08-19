# Migration record

This repository's `main` branch was merged into
[`A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline`](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline)
as that repo's `warehouse/` + `dags/` + `migrations/` subsystem.

- **Source:** this repo, `main` branch, commit `8073824` (2026-08-17).
- **Destination:** `A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline`, branch
  `claude/sec-edgar-pipeline-eval-sjt0v4`, commit `b6755ff`. That branch had
  not merged to the destination's default branch as of this writing — check
  whether it has by the time you read this, and use the current default-branch
  URL if so.
- **Full technical writeup, from the destination side:**
  [`docs/WAREHOUSE_INTEGRATION.md`](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline/blob/claude/sec-edgar-pipeline-eval-sjt0v4/docs/WAREHOUSE_INTEGRATION.md)
  in the destination repo. It has the architecture diagram, the full port map,
  and the destination repo's own list of what didn't move as-is. This document
  summarizes and links to it rather than duplicating it.

**Important caveat before you use the table below:** it maps `main`'s state at
the merge point. This repo also has an **open, unmerged pull request** —
[PR #1](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/pull/1), branch
`claude/sec-edgar-extraction-pipeline-ilcxmv` — that forked from `main` *before*
the merge and was never folded back in. Everything in that PR (a hash-chained
extraction audit trail, hybrid ML anomaly detection, metrics/benchmarking,
idempotent-upsert and retry-backoff work) is **not** reflected below and did
**not** travel to the destination repo. See "Work that has not moved" at the
bottom.

## Path mapping (what's in the destination repo today)

| Source path (this repo, `main`) | Destination path | Notes |
|---|---|---|
| `src/` | `warehouse/` | Renamed wholesale |
| `api/main.py` | `warehouse/api.py` | |
| `dags/` | `dags/` | Same name |
| `migrations/` | `migrations/` | Same name |
| `alembic.ini` | `alembic.ini` | Same name |
| `scripts/backfill.py` | `scripts/backfill.py` | Same name |
| `scripts/validate.py` | `scripts/validate.py` | Same name |
| `scripts/demo.py` | `scripts/demo.py` | Same name |
| `tests/test_api.py` | `tests/test_warehouse_api.py` | **Renamed** — the destination already had an unrelated `tests/test_api.py` for its own LLM-serving API, so this one was renamed to avoid colliding with it |
| other `tests/*.py` | `tests/*.py` | Same names, ported as-is |
| `docker-compose.yml` | `docker-compose.warehouse.yml` | Renamed — see port map below |

## Environment variable renames

| This repo (`main`) | Destination |
|---|---|
| `DATABASE_URL` | `WAREHOUSE_DATABASE_URL` (falls back to legacy `DATABASE_URL` if unset) |
| `REDIS_URL` | `WAREHOUSE_REDIS_URL` (falls back to legacy `REDIS_URL` if unset) |

Kept distinct from the destination repo's *other*, pre-existing subsystem
(LLM extraction), which has its own unrelated Postgres/Redis and env vars.

## Port changes

| | This repo (`main`) | Destination |
|---|---|---|
| Postgres | `5432` | `5433` |
| Redis | `6379` | `6380` |

Changed specifically so the warehouse subsystem doesn't collide with the
destination's other (LLM-serving) subsystem, which already used `5432`/`6379`.

## Dependency split

`apache-airflow` was pulled into its own `requirements-warehouse.txt` in the
destination — optional, only needed to run a real Airflow scheduler; the test
suite mocks Airflow out entirely (see the destination's `tests/test_dag.py`
docstring for why). Everything else this subsystem needs — `sqlalchemy`,
`alembic`, `lxml`, `requests`, `requests-mock`, `scikit-learn` — is in the
destination's main `requirements.txt` because its test suite genuinely imports
them.

## Known differences from a straight copy

These were real fixes made during the merge, not just a mechanical port — the
destination is not byte-identical to this repo's `main`.

1. **SQLite pool-kwargs bug, fixed in the destination only.** This repo's
   `api/main.py` hardcodes `pool_size`/`max_overflow` on engine creation, which
   raises `TypeError` against a `sqlite://` URL — this repo's own
   local-dev/testing pattern (e.g. `scripts/demo.py`). The destination's
   `warehouse/api.py` only applies those kwargs for non-SQLite URLs. **If this
   repo is ever revived standalone, this is a real latent bug here too** —
   documented, not fixed, since this repo is frozen.

2. **Test-collection-order env leak, exposed (and fixed) during the merge.**
   Renaming `tests/test_api.py` → `tests/test_warehouse_api.py` in the
   destination shifted pytest's alphabetical collection order relative to
   `tests/test_dag.py`, which exposed a cross-test env-var leak:
   `test_dag.py` sets `DATABASE_URL` via `os.environ.setdefault` at **module**
   level with no teardown. The SQLite-safety fix above is what actually
   resolves the symptom, but the same class of bug could resurface in this
   repo's own test suite if it's ever reordered (e.g. a new test file with a
   name alphabetically between `test_client` and `test_dag`).

## Work that has not moved

This repo's `main` line is **not** its only line of development. Branch
`claude/sec-edgar-extraction-pipeline-ilcxmv`
([PR #1](https://github.com/A-Kuo/sec-edgar-extraction-pipeline/pull/1))
forked from `main` before the merge above and independently added:

- A hash-chained extraction audit trail (`src/audit.py`, `extraction_audit`
  table, a Postgres trigger enforcing append-only writes, `GET /audit/{accession}`,
  `scripts/verify_audit_chain.py`) — design doc `docs/AUDIT_TRAIL_PLAN.md`.
- Hybrid ML anomaly detection (`src/ml/`: IsolationForest + deterministic
  rules, model registry, drift monitoring, `GET /anomalies/{ticker}`,
  `GET /model/current`, training/evaluation CLIs) — `MODEL_CARD.md`.
- Run metrics and benchmarking (`collect_run_metrics` DAG task,
  `metrics/run_metadata.json`, `scripts/benchmark_pipeline.py`, `make benchmark`).
- Full-jitter exponential backoff on `EdgarClient` retries, and a second,
  independent implementation of idempotent UPSERTs (`src/upsert.py`) — `main`
  has its own separate UPSERT implementation, so these two need reconciling,
  not a blind merge.

**None of this is in the destination repo.** PR #1 is open but unmerged, and
its diff against `main` currently conflicts (`mergeable_state: dirty`) —
resolving it needs to be additive, since it also *deletes* things `main`
gained independently after the fork point (`src/rag/`, the analytics-marts
migration, `scripts/demo.py`, and a few test files). See the comment thread on
PR #1 for the specific list.

Until PR #1 is resolved — either merged into this repo's `main` and then
ported into the destination, or ported into the destination directly — this
work exists only on that one branch. This repo should not be archived until
that's settled, so anyone relying on it isn't left with no path forward.

---

For all future work once this repo winds down, go to
[`A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline`](https://github.com/A-Kuo/Fine-Tuned-SEC-Filing-Extraction-Pipeline).
