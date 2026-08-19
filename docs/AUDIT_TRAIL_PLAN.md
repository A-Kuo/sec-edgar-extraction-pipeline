# Implementation plan: hash-chained, per-accession, DB-enforced audit trail

**Status:** Implemented, following the sequencing in §10. `src/audit.py`,
migration `2cf6396ba519`, the DAG wiring, the `GET /audit/{accession}`
endpoint, and `scripts/verify_audit_chain.py` all exist and are tested —
see `docs/DECISION_LOG.md` §9 for what shipped, what changed from this plan
during implementation (notably: the hash formula here was missing the
`detail` field, caught by a tamper-path test rather than by inspection —
see `src/audit.py`'s `compute_row_hash` docstring), and the commit history
for the sequence of PRs. This document is kept as the design rationale —
the *why* behind the schema and hash-chain choices — rather than removed,
since the code doesn't explain those trade-offs on its own.

**Why this is a separate document instead of a diff:** the other two gaps in
this audit (backoff jitter, idempotent UPSERTs) were each a bounded, single-PR
change. This one touches the schema, a DB-level trigger, multiple DAG stages,
the API, and a new CLI — pushing it through in the same sitting as the other
two risked a rushed implementation of the piece that most needs to be
correct, since its entire point is being trustworthy evidence in an audit.
Better to scope it properly and let it be its own reviewable PR.

## 1. The gap this closes

Audited finding (see commit history / PR description for the full audit):
`pipeline_audit` (`src/schema.py`) is real, and append-only *by convention*
— nothing in the codebase issues `UPDATE` or `DELETE` against it — but two
things fall short of a CMMC AU-2/AU-3/AU-12-grade audit trail:

1. **Granularity.** It has `run_id, stage, status, records_processed,
   error_message, created_at` — one row per *DAG stage per run*, not one row
   per *accession per extraction attempt*. There is no `accession_id` column
   at all, and no `system_id`/`user_id` column. You cannot currently answer
   "show me every extraction attempt against accession X" from this table.
2. **Enforcement.** Immutability is application discipline, not a database
   guarantee. A session with `UPDATE`/`DELETE` grants on `pipeline_audit` can
   silently rewrite history; nothing in the schema stops it. That is not
   non-repudiation.

## 2. Target schema

New table, additive — `pipeline_audit` is not renamed or removed, both serve
different granularities and both stay.

```sql
CREATE TABLE extraction_audit (
    id               BIGSERIAL PRIMARY KEY,
    run_id           TEXT NOT NULL,
    system_id        TEXT NOT NULL,       -- see §3 below
    accession_number TEXT,                -- NOT a FK -- see rationale below
    stage            TEXT NOT NULL,       -- download | parse | load (see §5)
    extraction_status TEXT NOT NULL,      -- success | failure | skipped
    detail           TEXT,                -- error message / free-text detail
    content_hash     CHAR(64),            -- sha256 of the raw/parsed content at this stage, nullable
    prev_row_hash    CHAR(64),            -- NULL only for the very first row ever written
    row_hash         CHAR(64) NOT NULL,   -- sha256 of this row's own fields + prev_row_hash
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_extraction_audit_accession_number ON extraction_audit (accession_number);
CREATE INDEX ix_extraction_audit_run_id ON extraction_audit (run_id);
CREATE INDEX ix_extraction_audit_created_at ON extraction_audit (created_at);
```

**Why `accession_number` is not a foreign key to `filings_raw`:** a CMMC
AU-3-grade record has to be able to log a *failed* extraction attempt for an
accession that never made it into `filings_raw` at all (a corrupt download, a
parse that threw before any row was ever built). A `REFERENCES
filings_raw(accession_number)` constraint would make that impossible to
record — exactly backwards for an audit trail whose most important job is
capturing failures, not just successes. Same pattern the existing
`pipeline_run_id` column on `filings_raw` already uses (an unconstrained
`TEXT`, deliberately).

Add this table's model to `src/schema.py` (`ExtractionAudit`, alongside the
existing `PipelineAudit`) and generate a migration the same way the two
existing migrations were generated (`alembic revision --autogenerate`) — do
**not** hand-edit `b17f52f92845` or `03255b5bee46`, both are already pushed
and public.

## 3. `system_id`

Out of scope to build real authenticated-principal plumbing in this pass —
there is no user-auth layer anywhere in this pipeline today, so inventing one
just for the audit trail would be scope creep. Instead:

- Read from a `SYSTEM_ID` environment variable, defaulting to something like
  `"edgar-extraction-pipeline"`.
- Document explicitly (in the module docstring and in `.env.example`) that a
  real deployment should set this to the authenticated identity of whatever
  is actually running the worker — a Vault AppRole ID, a Kubernetes service
  account name, an Airflow connection's login — and that the column exists
  now so that plumbing has somewhere to land later without another schema
  migration.

This is the honest scope: the column and the design are real; a fully wired
identity provider is explicitly future work, and the doc should say so rather
than imply more than is built.

## 4. The hash chain

**Row hash:**

```python
row_hash = sha256(
    (prev_row_hash or "")
    + "\x1f"
    + system_id
    + "\x1f"
    + (accession_number or "")
    + "\x1f"
    + run_id
    + "\x1f"
    + stage
    + "\x1f"
    + extraction_status
    + "\x1f"
    + created_at.isoformat()
    + "\x1f"
    + (content_hash or "")
    + "\x1f"
    + (detail or "")
).hexdigest()
```

`detail` is included deliberately: it's the one free-text field on this
table, most often an error message — exactly the content someone tampering
with a record would want to alter. An early implementation draft omitted it
from the hash, and a chain that verified as VALID after `detail` alone was
mutated is what caught the gap — see
`tests/test_audit.py::TestTamperDetection::test_altering_detail_field_alone_is_caught`.
Worth calling out here: the value of testing the tamper path directly,
rather than only testing that a clean chain verifies, is exactly this — a
happy-path-only test suite would have shipped this gap silently.

(`\x1f`, ASCII unit separator, matches the delimiter convention already
established in `src/upsert.py::compute_fact_hash` — reuse that convention,
don't invent a new one.)

**Why a hash chain and not just per-row hashes:** a per-row hash alone (no
chain) only proves a row hasn't been individually altered — it does nothing
to prove a row wasn't *deleted*, or that the sequence wasn't reordered.
Chaining each row to the previous one's hash means deleting or reordering any
row breaks every hash after it, which is what makes the *whole log*
verifiable, not just individual entries.

**Honesty about what this does and doesn't prove:** a plain hash chain proves
*internal consistency* — that the stored rows are exactly the rows that were
written, in the order they were written. It does **not** provide
cryptographic non-repudiation against an attacker who has both database
write access and read access to the verification code, because they could in
principle recompute a fresh, internally-consistent chain from scratch and
replace the whole table. Real non-repudiation needs an HMAC keyed with a
secret the writer holds and the verifier doesn't need write access to (or an
external anchor — e.g. periodically publishing the latest `row_hash` to
somewhere the pipeline itself cannot write, such as a separate audit-only AWS
account, or S3 Object Lock as referenced in the target CMMC narrative this
project is defending). That is real future work requiring a secrets manager
(Vault / AWS Secrets Manager) this project does not yet integrate — call this
out explicitly rather than quietly shipping a hash chain and implying it's
HMAC-grade. The chain as specified here is still a large, genuine
improvement over "nothing stops an UPDATE" — it is just not the final word.

**Concurrency:** every write has to read the current tail's `row_hash` before
computing its own, which serializes writers on this table. For this
pipeline's actual write volume (one Airflow worker, batches of filings, not
high-frequency independent writers) that's a non-issue. If this table's
volume ever grows enough for chain-write contention to matter, the standard
mitigation is partitioning the chain (e.g., one chain per `run_id`, or one
per `accession_number`) rather than one global chain — still fully
tamper-evident within each partition, just not comparable in insertion order
*across* partitions without also recording a global sequence number. Not
needed at current scale; noted here so the tradeoff is a documented decision,
not a surprise later.

Implementation: `session.execute(select(...).order_by(id.desc()).limit(1))`
inside the same transaction as the insert (or `SELECT ... FOR UPDATE` on
PostgreSQL if write concurrency ever increases enough to risk two
transactions reading the same tail before either commits — not needed yet,
but the hook to add it is exactly here).

## 5. Where this gets written

Wire `extraction_audit` writes into the DAG stages that actually operate
per-accession and have a clean success/failure per filing:

- **`download_raw_documents`** — one row per accession, `success` or
  `failure`, with `content_hash` = sha256 of the downloaded HTML (this can
  reuse the same `hashlib.sha256` pattern already used in
  `src/upsert.py::compute_fact_hash` and `src/ml/registry.py::sha256_file`).
- **`parse_xbrl_facts`** — one row per accession, `success` or `failure`.
- **`load_to_warehouse`** — one row per accession once its facts are
  upserted.

Deliberately **out of scope for the first pass**:
`fetch_new_filings` (operates on the whole batch before any single accession
is meaningful yet), `validate_quality_gates` and `score_anomalies` (both
already produce per-accession detail in other tables —
`QualityResult.missing_facts_by_accession` and `fact_anomalies` respectively
— so a redundant `extraction_audit` row there is lower priority; add later if
a real audit review specifically asks for it).

Add a `write_extraction_audit_batch(session, *, run_id, stage, system_id,
outcomes: dict[str, tuple[status, detail, content_hash]])` helper in the new
`src/audit.py` so the three DAG stages above call one function each rather
than hand-rolling the chain-read-then-insert dance three times.

## 6. Database-level immutability enforcement

This is the piece that upgrades "append-only by convention" to an actual
guarantee, for **both** `extraction_audit` and the existing `pipeline_audit`
(closing the enforcement gap on the table that already claims this property
today).

In the migration, PostgreSQL-only (guard on `op.get_bind().dialect.name ==
"postgresql"`; SQLite has no equivalent and the test suite runs on SQLite):

```sql
CREATE OR REPLACE FUNCTION prevent_audit_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'Table % is append-only: % is not permitted (CMMC AU-12 non-repudiation)',
        TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_pipeline_audit_immutable
    BEFORE UPDATE OR DELETE ON pipeline_audit
    FOR EACH ROW EXECUTE FUNCTION prevent_audit_mutation();

CREATE TRIGGER trg_extraction_audit_immutable
    BEFORE UPDATE OR DELETE ON extraction_audit
    FOR EACH ROW EXECUTE FUNCTION prevent_audit_mutation();
```

Alembic/SQLAlchemy have no first-class trigger support — write this as raw
`op.execute(...)` calls in `upgrade()`, with the matching `DROP TRIGGER` /
`DROP FUNCTION` in `downgrade()`.

**Testing this specifically needs live PostgreSQL** — SQLite can't run this
trigger syntax, so it cannot be covered by the SQLite-backed unit suite the
rest of this project tests against. `.github/workflows/ci.yml`'s
`dag-and-migrations` job already spins up a real `postgres:16-alpine` service
container for the migration-reversibility checks; add a step there,
immediately after `alembic upgrade head`, that attempts an `UPDATE` and a
`DELETE` against both tables and asserts both raise. That is the only place
in the whole test matrix this specific guarantee can be verified — say so in
a comment at that step, so nobody "simplifies" it out later assuming the
SQLite suite already covers it.

## 7. `src/audit.py`

New module, mirroring the style of `src/alerts.py` / `src/upsert.py`:

- `compute_row_hash(prev_row_hash, system_id, accession_number, run_id, stage, extraction_status, created_at, content_hash) -> str`
- `write_extraction_audit(session, *, system_id, accession_number, run_id, stage, extraction_status, detail=None, content_hash=None) -> ExtractionAudit`
  — reads the current chain tail, computes the new row's hash, inserts.
- `write_extraction_audit_batch(session, *, run_id, stage, system_id, outcomes) -> list[ExtractionAudit]`
  — the DAG-facing convenience wrapper described in §5.
- `AuditChainVerification` (dataclass): `valid: bool`, `total_rows: int`,
  `broken_at_id: int | None`, `broken_reason: str | None`.
- `verify_chain(session, table=ExtractionAudit) -> AuditChainVerification` —
  walks the table in `id` order, recomputes each row's hash from its stored
  fields, and checks both (a) the recomputed hash matches the stored
  `row_hash`, and (b) each row's `prev_row_hash` matches the previous row's
  `row_hash`. Returns where the chain broke, not just whether it's valid, so
  a caller can pinpoint the tampered/missing row.

## 8. API and CLI surface

- `GET /audit/{accession}` in `api/main.py` — returns `extraction_audit`
  history for one accession, newest first. This is the endpoint that
  directly answers "where did this number come from, and prove it" — the
  interview narrative this whole audit is checking claims for. Not cached,
  same reasoning as `/anomalies` and `/model/current`.
- `scripts/verify_audit_chain.py` — CLI wrapping `verify_chain()`, styled
  like `scripts/validate.py` (colour output, exit 0 on valid / 1 on a broken
  chain). This is the tool a CMMC assessor or an internal auditor actually
  runs.

## 9. Test plan

- `tests/test_audit.py` (SQLite, no trigger — the app-level hash-chain logic
  is fully testable without Postgres): chain construction, `verify_chain()`
  catching a tampered row's content, a deleted row, a reordered row, and the
  clean-chain case. Mirror the rigor of `tests/test_upsert.py`'s
  `TestWorkerRestartMidBatch` — write a `TestTamperDetection` class that
  actually mutates a committed row's `detail` field via a raw UPDATE (bypass
  the ORM, since SQLite won't have the trigger to stop it) and asserts
  `verify_chain()` reports `valid=False` at the right `broken_at_id`.
- CI-only, Postgres-backed (see §6): trigger actually blocks `UPDATE`/`DELETE`.
- `tests/test_scripts_ml.py`-style CLI tests for `verify_audit_chain.py`
  (valid chain -> exit 0, tampered -> exit 1).

## 10. Suggested sequencing

Mirrors how the backoff-jitter and UPSERT fixes were sequenced — small,
reviewable, each independently correct:

1. Schema + migration (table, columns, indexes) — no trigger yet, no DAG
   wiring. Get this reviewed and merged first since everything else depends
   on the shape being right.
2. `src/audit.py` (hash chain + verification) with full unit test coverage,
   against the schema from (1).
3. DB trigger, as a follow-up migration, plus the CI Postgres verification
   step from §6.
4. DAG wiring (the three stages in §5).
5. API endpoint + CLI + their tests.
6. Update `AGENTS.md` / `README.md` to describe the finished feature the same
   way the UPSERT and jitter work was documented — this doc should then be
   marked superseded/removed once the real docs cover it.

## 11. Open questions for whoever implements this

- Should `pipeline_audit` eventually be folded into `extraction_audit` (one
  table, `accession_number` nullable for the batch-level rows), or kept as
  two tables at different granularities permanently? Leaning toward keeping
  them separate — different retention/query patterns — but worth deciding
  explicitly rather than by accretion.
- Real `system_id` sourcing is deployment-specific (Vault AppRole? K8s
  service account? Airflow connection login?) — needs a decision from
  whoever owns the actual deployment target, not something to guess at in
  this codebase.
- Chain partitioning (per-run vs. global) — revisit only if write-volume
  data shows it's actually needed; don't build it speculatively.
