# Decision Log

This document exists to substantiate specific engineering claims with
specific evidence — a commit hash, a before/after metric, or a test that
would have failed under the old design and passes under the new one. Every
entry below is something that was actually found wrong in this repository
and actually corrected in it; none are hypothetical or reconstructed for
effect. Where a decision was deliberately *not* rushed into code, that's
recorded too, because knowing when not to ship a fix is part of the
judgment this log is meant to demonstrate.

**A note on scope, stated directly:** an earlier draft of this project's
documentation was asked to describe a prior *microservice* architecture that
was corrected into the current modular monolith. That history does not
exist. The project's original build specification (`AGENT2.md`, written
before this repository's first commit and later folded into `AGENTS.md`)
already laid out extraction, validation, and persistence as Python modules
within one Airflow-orchestrated deployable — not as independently deployed
services. Rather than invent a correction that never happened, this log
documents the architecture as what it actually was: a deliberate choice made
up front, with reasoning (§0), sitting alongside the corrections that *did*
happen (§1–8) and the one gap that was found and intentionally deferred
(§9).

Every commit hash below is verifiable with `git show <hash>` against this
repository.

---

## §0. Modular monolith — a deliberate choice, not a correction

**Decision:** extraction (`src/edgar_client.py`, `src/xbrl_parser.py`),
validation (`src/quality.py`, `src/ml/`), and persistence (`src/schema.py`,
`src/upsert.py`) live in one codebase, imported directly as Python modules
and orchestrated in-process by a single Airflow DAG (`dags/edgar_pipeline.py`)
— not as separately deployed services communicating over HTTP.

**Why this was the right call from the start, argued rather than assumed:**

- Airflow already owns retry policy, scheduling, and inter-stage dependency
  management. A service-per-stage architecture would either duplicate that
  machinery (each service reimplementing retry/backoff) or route it back
  through Airflow anyway — at which point the services are just Python
  functions with an HTTP hop and a serialization tax in between, not an
  independent unit of anything.
- The entire second half of this project's engineering effort (see §6–8
  below) is about *provenance*: proving which code, which model, and which
  database transaction produced a given number. That story is materially
  harder to tell across N independently deployed, independently versioned
  services than within one deployable with one database connection pool and
  one git SHA per running version. Fewer trust boundaries to secure and
  reason about is a feature for a CMMC-adjacent audit trail, not
  incidental.
- SEC EDGAR's rate limit (10 req/s) is one shared constraint
  (`src/edgar_client.py::_TokenBucket`). Enforcing that correctly across
  independently scaled services would need a distributed rate limiter
  coordinating between them — solving a problem a monolith doesn't have, in
  order to reintroduce it.

**Trade-off acknowledged, not ignored:** this doesn't let extraction (batch,
bursty, rate-limited by an external API) and serving (`api/main.py`, latency-
sensitive, read-heavy) scale independently. If that ever becomes a real
constraint, `api/main.py` is already the one clean seam — it talks only to
PostgreSQL and Redis, never imports anything from `dags/` or `src/ml/`
directly, and could be split into its own deployment without touching the
extraction path at all. Not needed today; not designed around a hypothetical
need either.

**The alternative, sketched out rather than asserted:**
[`docs/MICROSERVICE_ALTERNATIVE.md`](MICROSERVICE_ALTERNATIVE.md) works
through what a microservice decomposition of this same system would
actually look like — service boundaries, data ownership, API contracts, a
deployment sketch — and argues the rejection using the specific defects in
§1–9 below as evidence, rather than general monolith-vs-microservice
talking points. It also names the one place a service split would be a
genuine improvement (isolating the audit trail's write credentials), so the
rejection of the rest is a comparison, not a dismissal.

---

## §1. Airflow 3 incompatibility hidden by a false-green test suite

**Commit:** `27772ca`

**What was wrong:** `requirements.txt` pinned `apache-airflow>=2.8.0` with
no upper bound. That resolved to Airflow 3.3 at install time. Airflow 3
removed the DAG constructor's `schedule_interval` argument and relocated
`PythonOperator`, `TriggerRule`, and the exception classes to new module
paths. The DAG could not be imported by a real Airflow installation.

**Why it went unnoticed — this is the actual finding, not the version bump
itself:** `tests/test_dag.py` replaces `sys.modules["airflow"]` and its
submodules with hand-written mock classes *before* importing the DAG module,
specifically so the structural tests (task count, dependency edges, trigger
rules) run in milliseconds with no Airflow metastore. That is a reasonable
test design on its own terms. Its blind spot is that it never once imports
the DAG against a real Airflow — so the suite reported 131/131 passing while
the module the tests were nominally covering would have failed to parse in
any actual scheduler.

**Fix:** version-compatibility shims in `dags/edgar_pipeline.py`
(`try: from airflow.sdk.exceptions import ... except ImportError: from
airflow.exceptions import ...`, and equivalently for the operator, trigger
rule, and the `schedule_interval` → `schedule` rename).

**Verification that closes the actual gap, not just the symptom:**
`tests/test_dag_import.py` imports the module in a *subprocess*, against
whichever Airflow is actually installed, with no mocking — and separately
loads the `dags/` folder through Airflow's own `DagBag`, the same loader the
scheduler uses. CI (`.github/workflows/ci.yml`) runs this against a live
Airflow install on every push. This is the test that would have caught the
original defect; `tests/test_dag.py`'s existence is not sufficient on its
own, and the two files' docstrings each say so explicitly.

---

## §2. PSI drift monitor manufacturing false alerts on skewed data

**Commit:** `27772ca`

**What was wrong:** `compute_psi()`'s original equal-width binning divides
the combined value range into N equal-width bins. Every financial magnitude
this pipeline handles is skewed (log-normal-ish), so equal-width bins
concentrate nearly all the mass into one or two bins and leave the rest
almost empty; the epsilon floor added to avoid `log(0)` then manufactures
PSI out of those near-empty bins. Measured: two independent samples drawn
from the *identical* synthetic generator (not two different populations)
scored PSI 0.37 on `log_assets` — above the 0.25 ALERT threshold — purely
from binning artifact.

**Fix:** added `strategy="quantile"` to `compute_psi()` — equal-*frequency*
bins computed from the baseline sample, so no bin is empty by construction.
The default (`strategy="uniform"`) was left unchanged for existing callers
so nothing already calibrated against it silently shifted; the new ML drift
monitor (`src/ml/monitoring.py`) opts into quantile binning explicitly, with
the reasoning recorded in its module docstring.

**Verification:** `.github/workflows/ml.yml`'s drift sanity check is
two-sided on purpose — it asserts the monitor stays quiet on an equivalent
population *and* fires on a genuine, deliberately injected 1000x systematic
shift. A monitor that only proves "doesn't crash" is decoration; this proves
it discriminates.

---

## §3. Anomaly model alone had 0.17 recall on a real, specific failure mode

**Commit:** `27772ca`

**What was wrong:** every filing in training has full XBRL fact coverage —
`fact_coverage == 1.0` for the whole training set — so that feature has zero
variance, gets flattened by `StandardScaler`, and IsolationForest has
structurally no way to detect a *dropped* required fact. Measured directly:
recall on that specific corruption type (via `src/ml/evaluation.py`'s
corruption-injection harness) was 0.17.

**Fix:** `src/ml/rules.py` — a small set of deterministic plausibility
checks (assets/revenue must be positive, EPS × shares must reconcile with
net income within tolerance, leverage must fall in a bounded range, etc.)
run alongside the model. A filing's score is `max(model_score, rule_score)`,
and every score records which half fired and why.

**Measured impact** (`tests/test_ml_model.py`, `scripts/evaluate_model.py`
against the same injected-corruption harness): overall recall 0.43 → 0.73,
precision 0.30 → 0.55, ROC-AUC 0.76 → 0.94.

---

## §4. Threshold miscalibration — a 5%-contamination model flagging 10% of clean data

**Commit:** `27772ca`

**What was wrong:** the original score normalization did a plain min-max
rescale of the IsolationForest decision function into `[0, 1]`. That placed
`threshold=0.6` at roughly the training set's 90th percentile rather than
its 95th — so a detector explicitly configured for 5% contamination flagged
~10% of its own clean training data. Every one of those flags was, by
construction, a false positive.

**Fix:** `AnomalyDetector._normalise()` now pins the training set's
`contamination`-quantile decision value to exactly `threshold`, so the
configured contamination rate *is* the observed flag rate on in-distribution
data.

**Verification:**
`tests/test_ml_model.py::TestCalibration::test_training_flag_rate_matches_contamination`,
parametrized over three contamination rates (0.02, 0.05, 0.10), asserts the
observed flag rate matches the configured one within 2 percentage points.

---

## §5. `BigInteger` primary keys silently broke ORM-level writes under SQLite

**Commit:** `27772ca`

**What was wrong:** SQLAlchemy only grants SQLite's autoincrement
rowid-alias behavior to a primary key typed exactly `Integer`; a
`BigInteger` primary key compiles to `BIGINT`, which SQLite does not treat
as a rowid alias. Every ORM-level insert into a table with a `BigInteger`
surrogate key failed with a `NOT NULL constraint failed: <table>.id` error
under SQLite — while working fine against PostgreSQL's `BIGSERIAL`. This was
invisible for as long as it was, because every prior test built rows with
raw `text()` SQL rather than ever instantiating the SQLAlchemy model classes
directly — it surfaced only once real ORM-level schema tests
(`tests/test_schema.py`) were added and actually tried to insert through the
ORM.

**Fix:** `BigInteger().with_variant(Integer, "sqlite")` on every surrogate
primary key — production behavior on PostgreSQL is unchanged; SQLite gets
the rowid-alias treatment it needs to make ORM writes work.

**Verification:** `tests/test_schema.py` inserts through the ORM directly
for every model, not through raw SQL — the class of test that would have
caught this the first time.

---

## §6. Documented migration step created nothing

**Commit:** `27772ca`

**What was wrong:** `migrations/versions/` contained only a `.gitkeep`. The
README's documented setup step, `alembic upgrade head`, ran successfully and
created zero tables. A fresh clone following the documented setup would come
up with an empty database and fail at the first query at runtime, not at
setup — a much more confusing failure to debug.

**Fix:** generated and committed the real initial migration
(`b17f52f92845`), later followed by `03255b5bee46` (see §8) — new revisions,
not edits to a migration already pushed and public.

**Verification:** CI's `dag-and-migrations` job applies migrations to a live
`postgres:16-alpine` service container and explicitly asserts every expected
table exists, checks the migration is reversible
(`alembic downgrade base && upgrade head`), and runs `alembic check` to
catch any future model/migration drift before merge.

---

## §7. Deterministic exponential backoff — a self-inflicted rate-limit-ban risk

**Commit:** `c063ca1`

**Context:** found via a direct audit against the specific claim
"exponential backoff with jitter."

**What was wrong:** `EdgarClient._get()`'s retry loop backed off with a
purely deterministic, doubling delay (`backoff = min(backoff * 2, 60)`),
slept via a bare `time.sleep(backoff)`. Given identical inputs (the same
rate-limit response, the same retry count), every worker computed the
identical delay. Multiple workers hitting the same SEC EDGAR rate limit at
the same moment — the normal case for a scheduled batch pipeline — would
retry in lockstep: a synchronized wave against an endpoint that had just
asked everyone to back off. `Retry-After` was honored, but slept exactly as
given, with the identical lockstep problem across workers given the same
header value.

**Fix:** replaced with full-jitter backoff (`sleep = uniform(0, min(cap,
base * 2**attempt))`, per the AWS Architecture Blog's 2015 analysis of this
exact failure mode). `Retry-After` is still honored as a floor, with a few
seconds of jitter layered on top so identical server-given delays don't
resynchronize workers into the next wave.

**Verification:**
`tests/test_client.py::test_two_clients_retrying_simultaneously_do_not_lockstep`
instantiates two independent `EdgarClient`s, has both hit the same 503, and
asserts their retry delays differ — a direct test of the property that
matters, not just that a jitter function exists somewhere.

---

## §8. Blind INSERTs — no idempotency under the pipeline's own retry policy

**Commit:** `c0aaff8`

**Context:** found via a direct audit against the specific claim
"idempotency (UPSERTs)."

**What was wrong, concretely:**

- `_bulk_insert_facts` called `session.bulk_save_objects(objects)` — a blind
  bulk INSERT — against `financial_facts`, a table with no natural-key
  constraint, only a surrogate autoincrement `id`. The DAG's own
  `default_args` set `retries=2`. A worker that died *after* a batch
  committed but *before* Airflow recorded the task as successful would, on
  retry, resubmit the identical batch and duplicate every fact in it.
- `_persist_raw_filing` used check-then-insert (`session.get()` then a
  conditional `add()`) — not atomic, and a genuine time-of-check-to-
  time-of-use race under concurrent workers.
- `_persist_anomaly_scores` deleted and recreated its `ModelRun` row on
  every retry — functionally idempotent, but churned the row's `id` on
  every retry and briefly left the run with zero `fact_anomalies` rows
  between the delete and the re-insert.

**Fix:** `src/upsert.py` — every write above replaced with a single atomic
`INSERT ... ON CONFLICT` statement:

| Table | Conflict key | Action |
|---|---|---|
| `financial_facts` | `fact_hash` (SHA-256 of accession + concept + period + segment) | `DO UPDATE` |
| `filings_raw` | existing `accession_number` PK | `DO NOTHING` |
| `model_runs` | new `(run_id, model_version)` unique constraint | `DO UPDATE` |
| `fact_anomalies` | existing `(model_run_id, accession_number)` constraint | `DO UPDATE` |

Migration `03255b5bee46` backfills `fact_hash` for any pre-existing rows
using the *real* `compute_fact_hash()` (imported, not reimplemented in SQL —
a hand-copied version would silently drift from the one the application
writes with the moment either changed), and defensively de-duplicates
`model_runs` before adding its new constraint rather than assuming the
invariant already held.

**Verification, specifically designed to reproduce the failure, not just
test the happy path:** `tests/test_upsert.py::TestWorkerRestartMidBatch`
replays a full batch, and separately a *partial* batch (simulating a crash
between the second and third fact committing), and asserts both replays
converge to exactly one row per fact. `_insert()` dispatches
`sqlalchemy.dialects.postgresql.insert` vs. `.sqlite.insert` by the bound
engine's dialect name — both expose an identical `on_conflict_do_update()` /
`on_conflict_do_nothing()` API (confirmed empirically: SQLite 3.24+
supports it; this environment's bundled `sqlite3` is 3.45), so these tests
run the *real* conflict-resolution SQL against SQLite rather than mocking
it, and the identical Python runs against PostgreSQL in production.

---

## §9. Audit trail claimed "immutable" but enforced only by convention — found, designed, deliberately not rushed

**Status:** identified and fully designed; **not yet implemented**. See
[`docs/AUDIT_TRAIL_PLAN.md`](AUDIT_TRAIL_PLAN.md) for the complete design.

**What was found:** `pipeline_audit` is real, and nothing in the codebase
ever issues `UPDATE` or `DELETE` against it — but nothing at the *database*
level stops one either; the guarantee is application discipline, not a
database guarantee. Separately, it is stage-level (one row per DAG stage per
run), not per-accession — there is no `accession_id` or `system_id` column,
so "show every extraction attempt against accession X" is not answerable
from it today, which is short of what a CMMC AU-3-grade record needs.

**Why this entry stops at "designed" instead of "fixed," unlike §7 and §8
above:** the fix touches the schema, a PostgreSQL-only trigger, three DAG
stages, a new API endpoint, and a new CLI — enforcing immutability
correctly (a hash chain with an honestly-stated limit on what it does and
doesn't prove cryptographically, plus a trigger that can only be verified
against live PostgreSQL, not the SQLite the rest of the suite runs against)
is exactly the kind of change that goes wrong when pushed through quickly
alongside two other fixes in the same sitting. `docs/AUDIT_TRAIL_PLAN.md`
specifies the schema, the hash-chain construction, the trigger SQL, which
DAG stages get wired and why others are deliberately deferred, the test
plan, and a suggested sequencing into independently reviewable pull
requests — written so it can be implemented directly without redoing the
design work, whenever it's picked up.

This entry is itself part of the point of this log: not every gap found
gets shipped in the sitting it was found in, and that's a decision, not an
omission.
