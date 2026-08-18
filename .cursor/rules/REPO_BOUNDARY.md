# Repository Boundary Contract

Multi-repository agent rules for SEC EDGAR extraction across two
codebases. Read this before acting on any task.

## Repositories

| Repo | Scope | Branch prefix |
|------|-------|---------------|
| **sec-edgar-extraction-pipeline** | iXBRL-tagged facts, deterministic extraction, EDGAR ingestion, rate limiting, amendment chains, quality gates, audit trail | `claude/sec-edgar-extraction-pipeline-<suffix>` |
| **Fine-Tuned-SEC-Filing-Extraction-Pipeline** | Narrative extraction (MD&A, footnotes, non-GAAP), model fine-tuning, prompt engineering, LLM inference, GPU serving | `claude/fine-tuned-llm-<suffix>` |

Suffix is a short task name (e.g. `claude/sec-edgar-extraction-pipeline-add-ifrs-tag`,
`claude/fine-tuned-llm-risk-factor-adapter`).

## Routing Rules

**Task touches ONLY sec-edgar features** (XBRL parsing, facts
validation, anomaly detection, EDGAR API, quality gates, caching,
DAG orchestration, API serving of tagged facts):
- Work entirely in sec-edgar-extraction-pipeline
- Branch: `claude/sec-edgar-extraction-pipeline-<short-name>`

**Task touches ONLY narrative extraction** (model fine-tuning,
prompt engineering, LLM inference, confidence scoring, untagged
table extraction):
- Work in Fine-Tuned-SEC-Filing-Extraction-Pipeline
- Branch: `claude/fine-tuned-llm-<short-name>`

**Task spans both repos** (e.g. "extract all facts from filing ABC123"):
- sec-edgar agent: download and parse XBRL facts, record in `financial_facts`
- fine-tuned agent: extract narrative facts, record in `narrative_facts_table`
- Handoff via shared `run_id` in `run_metadata.json`
- Final: merged facts view in `/facts` endpoint

## Shared Database

Both repos write to the same PostgreSQL instance.

### sec-edgar-extraction-pipeline owns

| Table | Purpose |
|-------|---------|
| `filings_raw` | Raw HTML/XBRL landing zone, one row per accession |
| `financial_facts` | Parsed iXBRL facts (implicitly `method='xbrl'` until `method` column is added) |
| `filing_versions` | Amendment chain tracking |
| `pipeline_audit` | Append-only audit trail for XBRL pipeline stages |

### Fine-Tuned-SEC-Filing-Extraction-Pipeline owns

| Table | Purpose |
|-------|---------|
| `llm_inference_log` | LLM extraction run metadata |
| `narrative_facts_table` | Facts extracted from unstructured prose (`method='llm'`) |

### Cross-repo consistency checks

Before merging either repo:
- Run `pytest` locally to confirm no regressions
- Check `run_metadata.json` for both repos' metrics
- Ensure `pipeline_audit` and `llm_inference_log` are consistent
  (same `run_id`, sequential timestamps) for cross-repo runs

## Precedence Rule

An `llm` fact **never** overwrites an `xbrl` fact for the same
natural key `(accession_number, fact_name, period_end, segment)`.
XBRL always wins — the filer tagged it under penalty of law.
The reverse is permitted: an `xbrl` fact arriving after an `llm`
fact replaces it.

## Handoff Surface

The LLM repo consumes from this repo's database tables:
- `filings_raw` — filing documents (raw HTML) for narrative extraction
- `financial_facts` — existing XBRL facts for precedence enforcement
- `filing_versions` — amendment chains to resolve canonical accessions
- `pipeline_audit` — run status to gate LLM extraction on completed ingestion

See [docs/BOUNDARY.md](../../docs/BOUNDARY.md) for the full scope
contract, SQL query contracts, and architectural details.

## Commit Conventions

- Commit messages must NOT include model name or "Claude/Cursor"
  attribution — only technical description of the change.
- Commit early to separate branches.
- Avoid force-pushing unless explicitly approved by the user.
- One logical change per commit.
