# Extraction Boundary

Two repositories, one seam. The seam is whether the SEC filer
machine-tagged the number.

## Repositories

### sec-edgar-extraction-pipeline (this repo) — TAGGED

Owns: iXBRL-tagged facts. Ingestion, rate limiting, amendment
chains, Postgres, audit trail, quality gates, anomaly scoring.

Never: runs a language model in the extraction path. Never: extracts
financial data from unstructured prose.

Emits: filing documents + deterministic facts, `method='xbrl'`.

### Fine-Tuned-SEC-Filing-Extraction-Pipeline — UNTAGGED

Owns: extraction from narrative — MD&A, footnotes, non-GAAP
reconciliations, untagged tables. Training, eval, GPU serving.

Never: implements EDGAR ingestion, rate limiting, or amendment
logic. Consumes those from this repo.

Emits: facts marked `method='llm'` with a confidence score and
model version.

## Precedence rule

An `llm` fact **never** overwrites an `xbrl` fact for the same
natural key `(accession_number, fact_name, period_end, segment)`.
XBRL always wins. The reverse is permitted: an `xbrl` fact arriving
after an `llm` fact replaces it.

## What this repo owns

| Concern | Owned here | Notes |
|---|---|---|
| EDGAR API access (rate limiting, User-Agent, retry) | Yes | `src/edgar_client.py` |
| Filing metadata ingestion | Yes | `filings_raw` table, DAG stages 1–2 |
| Raw document storage (`raw_html`, `raw_xbrl`) | Yes | Landing zone for both repos |
| iXBRL fact extraction | Yes | `src/xbrl_parser.py`, deterministic |
| Quality gates (completeness, PSI drift) | Yes | `src/quality.py` |
| Amendment chain resolution | Yes | `filing_versions` table |
| Audit trail | Yes | `pipeline_audit` table, append-only |
| Redis caching | Yes | `src/cache.py` |
| API serving of XBRL facts | Yes | `api/main.py` — `/filings`, `/filing`, `/facts`, `/trigger` |
| Narrative/prose extraction | **No** | LLM repo's scope |
| Model training, fine-tuning, eval | **No** | LLM repo's scope |
| GPU serving / inference | **No** | LLM repo's scope |
| Embedding indexes, vector stores | **No** | LLM repo's scope |

## What this repo does NOT do

- **No extraction from unstructured text.** If a number is not
  machine-tagged in iXBRL, this repo does not extract it. MD&A
  narrative, footnote disclosures, non-GAAP reconciliation tables,
  and any other prose-embedded figures are out of scope.
- **No language models in the extraction path.** The XBRL parser
  (`src/xbrl_parser.py`) is deterministic `lxml`; no model
  inference, no embeddings, no LLM calls.
- **No ML dependencies.** This repo's `requirements.txt` must not
  pull in `torch`, `transformers`, `sentence-transformers`,
  `openai` (as a hard dependency), or any other ML/inference
  library. `scikit-learn` is present solely for TF-IDF in the
  lightweight lookup aid (`src/rag/`), which is not an extraction
  component — see note below.

## The `/ask` endpoint — a lookup aid, not an extraction system

`GET /ask/{ticker}` and `src/rag/` provide a keyword-based lookup
over the raw filing text already stored in `filings_raw.raw_html`.
This is a **read-only research aid**: it retrieves and quotes
existing filing prose for human review. It does not extract
structured facts, does not write to `financial_facts`, and does not
produce data that feeds downstream.

It is explicitly **not** the narrative extraction system that the
LLM repo owns. The distinction:

| | `/ask` (this repo) | LLM extraction (other repo) |
|---|---|---|
| Purpose | Help a human find a passage in a filing | Extract structured facts from prose |
| Output | A quoted snippet + citation | A row in `financial_facts` with `method='llm'` |
| Writes to `financial_facts`? | No | Yes |
| Uses a trained model? | No (TF-IDF only) | Yes (fine-tuned LLM) |
| Deterministic? | Yes | No (model inference) |
| Confidence score? | Retrieval relevance only | Extraction confidence per fact |

If `/ask` ever needs to become a smarter retrieval system (e.g.
embeddings, reranking), that work belongs in this repo only if it
remains a read-only lookup aid. If it starts producing structured
facts from prose, that work belongs in the LLM repo.

## Handoff surface — what this repo emits for the LLM repo

The LLM repo consumes from this repo's database. The contract is:

### 1. Filing documents

```sql
SELECT accession_number, cik, ticker, filing_type, filing_date,
       period_of_report, raw_html
FROM   filings_raw
WHERE  ticker = :ticker
```

The LLM repo reads `raw_html` (and eventually the primary filing
document once the known ingestion gap is closed — see AGENTS.md)
to run its own extraction over narrative sections.

This repo is responsible for:
- Populating `filings_raw` via the Airflow DAG
- Maintaining amendment chains (`filing_versions`)
- Ensuring `raw_html` contains the actual filing document (tracked
  gap: currently stores the index page, not the primary document)

The LLM repo is responsible for:
- Parsing the HTML into the sections it needs (MD&A, footnotes, etc.)
- Running its own chunking/extraction pipeline over that text
- Handling its own model versioning and confidence thresholds

### 2. XBRL-extracted facts (for precedence enforcement)

```sql
SELECT accession_number, fact_name, fact_value, unit,
       period_start, period_end, segment
FROM   financial_facts
WHERE  accession_number = :accession
```

The LLM repo queries existing XBRL facts before writing its own
`method='llm'` facts, to enforce the precedence rule: if an XBRL
fact already exists for the same natural key, the LLM fact is
suppressed or marked as redundant.

### 3. Amendment resolution

```sql
SELECT accession_number, is_amendment, superseded_by
FROM   filing_versions
WHERE  cik = :cik AND filing_type = :type
       AND period_of_report = :period
```

The LLM repo uses this to determine which accession is the
canonical (non-superseded) version before extracting from it.

### 4. Pipeline run context

```sql
SELECT run_id, stage, status, created_at
FROM   pipeline_audit
WHERE  run_id = :run_id
ORDER  BY created_at
```

Optional. The LLM repo may read audit rows to determine whether
a given filing has been fully ingested and validated before
attempting extraction.

## Shared database — table ownership

Both repos write to the same PostgreSQL instance. Table ownership
is strict: each table has exactly one writer.

### This repo owns

| Table | Purpose |
|-------|---------|
| `filings_raw` | Raw HTML/XBRL landing zone, one row per accession |
| `financial_facts` | Parsed iXBRL facts (implicitly `method='xbrl'`) |
| `filing_versions` | Amendment chain tracking |
| `pipeline_audit` | Append-only audit trail for XBRL pipeline stages |

### LLM repo owns

| Table | Purpose |
|-------|---------|
| `llm_inference_log` | LLM extraction run metadata |
| `narrative_facts_table` | Facts extracted from unstructured prose (`method='llm'`) |

### Cross-repo run coordination

When a task spans both repos (e.g. "extract all facts from filing
ABC123"), both pipelines use the same `run_id`. The XBRL pipeline
records its stages in `pipeline_audit`; the LLM pipeline records
its stages in `llm_inference_log`. Before merging either repo,
verify that timestamps are sequential and both sides completed
for any shared `run_id`.

Coordination metadata lives in `run_metadata.json` (checked by
both repos' test suites).

## What this repo does NOT provide

- **No client library for the LLM repo.** The handoff surface is
  the database tables above, queried directly. Building a Python
  client, SDK, or API wrapper for the LLM repo to use is that
  repo's responsibility, not this one's.
- **No `method` column on `financial_facts`.** The current schema
  does not distinguish XBRL-sourced facts from LLM-sourced facts.
  When the LLM repo is ready to write facts, the `method` column
  (plus `confidence` and `model_version`) should be added via a
  migration in this repo, since the table lives here. Until then,
  all rows in `financial_facts` are implicitly `method='xbrl'`.
- **No ML dependencies or GPU infra.** This repo runs on commodity
  hardware with no GPU requirement. The LLM repo owns all model
  serving infrastructure.

## Agent conventions

For branch naming, commit rules, and multi-repo routing, see
[.cursor/rules/REPO_BOUNDARY.md](../.cursor/rules/REPO_BOUNDARY.md).
