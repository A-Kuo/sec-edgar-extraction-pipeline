# Model Card — Extraction Anomaly Detector

This card describes the model *design and evaluation methodology*. Measured
metrics are deliberately **not** reproduced here: every trained version carries
its own generated card at `models/<version>/MODEL_CARD.md`, rendered by
`src.ml.registry.render_model_card` from that version's real metadata. A number
copied into this file would go stale on the next training run.

To read the numbers for the currently promoted model:

```bash
cat "models/$(cat models/PRODUCTION)/MODEL_CARD.md"
```

---

## Overview

| Field | Value |
|---|---|
| Task | Flag filings whose *extracted* facts are internally inconsistent |
| Architecture | `StandardScaler` → `IsolationForest`, combined with deterministic rules |
| Implementation | [`src/ml/model.py`](src/ml/model.py), [`src/ml/rules.py`](src/ml/rules.py) |
| Registry | Content-addressed, [`src/ml/registry.py`](src/ml/registry.py) |
| CI gate | [`scripts/evaluate_model.py`](scripts/evaluate_model.py), run by `.github/workflows/ml.yml` |

## Intended use

Produce a **ranked review queue** for data-quality triage: given the facts this
pipeline extracted from a filing, surface the ones that do not hang together, so
a human looks at them before the numbers are trusted downstream.

## Out of scope

- **Not an extraction step.** Scores never modify a filed value. XBRL parsing
  stays deterministic and remains the sole source of reported numbers.
- **Not a fraud or misstatement detector.** A flag means *"these extracted
  numbers do not hang together"* — most often a parsing defect on our side, not
  an assertion about the filer.
- **Not an investment signal.**

## Why this architecture

**Why unsupervised.** Labelled extraction errors do not exist at usable volume;
nobody maintains a corpus of "10-Ks we parsed wrong." IsolationForest needs only
the unlabelled feature matrix, isolates outliers in few splits rather than
modelling the dense normal region, and is cheap enough to score a full backfill.
The cost is that `contamination` is an assumption rather than a learned
quantity, so it is an explicit constructor argument and is recorded in every
version's metadata.

**Why rules as well.** The forest alone measured 0.17 recall on dropped facts,
for a structural reason rather than a tuning one: every training filing has full
fact coverage, so that column has zero variance, `StandardScaler` flattens it,
and the signal is gone before any tree sees it. `src/ml/rules.py` asserts the
invariants that must hold in a well-formed filing; the forest catches what
nobody wrote a rule for.

**Why `max` and not a weighted blend.** A filing's score is the **maximum** of
the two components. A fired rule is a positive assertion of a defect; averaging
it against a calm model score would let the forest veto a known break. Each
score records which half fired.

## Evaluation methodology — and its limits

Because real labelled errors are unavailable, evaluation **injects synthetic
corruptions** into known-good filings and measures whether the detector surfaces
them. Four corruption kinds, defined in `src/ml/evaluation.py`:

| Kind | What it simulates |
|---|---|
| `scale_error` | A misread `scale` attribute — value off by orders of magnitude |
| `sign_flip` | A sign or parenthesised-negative misparse |
| `dropped_fact` | A required concept missing from the extraction |
| `eps_mismatch` | EPS that fails to reconcile with net income and share count |

**The honest caveat, stated in the module itself:** the resulting recall is
*"recall against these injected defects at this flag rate."* It is a real
measurement of a real capability, but it is not a claim about performance on the
distribution of extraction errors that occur in production, which is unknown.
Treat these as regression-detection metrics, not as a vendor benchmark.

## Shipping gate

`scripts/evaluate_model.py` runs in CI and exits non-zero on failure. A candidate
must clear absolute floors **and** must not regress against the promoted model:

| Metric | Floor |
|---|---|
| Recall | 0.60 |
| Precision | 0.35 |
| ROC AUC | 0.85 |

Regression tolerance against the promoted baseline is 0.05 — this exists so that
a series of individually-acceptable models cannot ratchet quality downward one
tolerable step at a time.

## Reproducibility

`random_state` is set on the estimator and defaults to `DEFAULT_SEED`. Training
twice on the same rows produces the same forest and therefore the same artifact
hash — the property the registry relies on to prove which model produced a given
score. Every registered version records the artifact SHA-256, the SHA-256 of the
exact feature matrix it was trained on, the git commit, library versions, seed,
and hyperparameters.
