"""
Train and register an anomaly detection model.

Usage
-----
    # Train on synthetic data (no database needed — this is what CI runs)
    python -m scripts.train_model --synthetic --promote

    # Train on the warehouse
    python -m scripts.train_model --since 2020-01-01 --promote

    # Reproduce a specific registered version
    python -m scripts.train_model --synthetic --seed 42 --contamination 0.05

Reproducibility
---------------
Every input that can change the artifact is an explicit flag with a fixed
default: the data seed, the model seed, the contamination rate, the tree count.
Two runs with the same flags on the same commit produce the same artifact hash,
which is what lets the registry treat that hash as an identity rather than a
checksum. The run prints the hash so it can be compared against a previous one.

Environment variables
---------------------
    DATABASE_URL           PostgreSQL connection string (not needed with --synthetic)
    MODEL_REGISTRY_ROOT    Registry directory (default: models)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("train_model")

DEFAULT_HOLDOUT_FRACTION = 0.2


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="train_model",
        description="Train, evaluate, and register an anomaly detection model.",
    )

    source = p.add_argument_group("data source")
    source.add_argument(
        "--synthetic",
        action="store_true",
        help="Train on generated filings instead of the warehouse. Required in CI, "
        "which has no database.",
    )
    source.add_argument(
        "--companies", type=int, default=200, help="Synthetic companies (default: 200)."
    )
    source.add_argument(
        "--years", type=int, default=6, help="Synthetic years per company (default: 6)."
    )
    source.add_argument(
        "--data-seed", type=int, default=7, help="Synthetic data seed (default: 7)."
    )
    source.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="Warehouse mode: only train on filings filed on or after this date.",
    )
    source.add_argument(
        "--limit", type=int, help="Warehouse mode: cap the number of filings loaded."
    )

    model = p.add_argument_group("model")
    model.add_argument("--seed", type=int, default=42, help="Model seed (default: 42).")
    model.add_argument(
        "--contamination",
        type=float,
        default=0.05,
        help="Expected anomaly rate; sets the flag rate directly (default: 0.05).",
    )
    model.add_argument(
        "--n-estimators", type=int, default=200, help="Trees in the forest (default: 200)."
    )
    model.add_argument(
        "--threshold",
        type=float,
        default=0.6,
        help="Score at or above which a filing is flagged (default: 0.6).",
    )

    evaluation = p.add_argument_group("evaluation")
    evaluation.add_argument(
        "--holdout",
        type=float,
        default=DEFAULT_HOLDOUT_FRACTION,
        help=f"Fraction held out for evaluation (default: {DEFAULT_HOLDOUT_FRACTION}).",
    )
    evaluation.add_argument(
        "--corruption-rate",
        type=float,
        default=0.10,
        help="Fraction of held-out filings to corrupt (default: 0.10).",
    )
    evaluation.add_argument(
        "--eval-seed", type=int, default=1337, help="Corruption seed (default: 1337)."
    )

    output = p.add_argument_group("output")
    output.add_argument(
        "--registry",
        default=os.getenv("MODEL_REGISTRY_ROOT", "models"),
        help="Registry root directory (default: models).",
    )
    output.add_argument(
        "--promote",
        action="store_true",
        help="Point PRODUCTION at the newly registered version.",
    )
    output.add_argument(
        "--no-register",
        action="store_true",
        help="Train and evaluate but do not write to the registry.",
    )
    output.add_argument(
        "--metrics-out",
        metavar="PATH",
        help="Write evaluation metrics as JSON (used by the CI gate).",
    )
    output.add_argument("--notes", default="", help="Free-text note stored in metadata.")

    return p.parse_args()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_synthetic(args: argparse.Namespace) -> tuple[list[dict], dict[str, dict]]:
    from src.ml.synthetic import generate_corpus

    corpus = generate_corpus(
        n_companies=args.companies,
        years_per_company=args.years,
        seed=args.data_seed,
    )
    return corpus.facts, corpus.filings


def _load_warehouse(args: argparse.Namespace) -> tuple[list[dict], dict[str, dict]]:
    """Load facts and filing metadata from PostgreSQL."""
    from sqlalchemy import create_engine, text

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit(
            "DATABASE_URL is not set. Set it, or pass --synthetic to train without a database."
        )

    engine = create_engine(database_url, pool_pre_ping=True)
    where = "WHERE fr.filing_date >= :since" if args.since else ""
    params: dict[str, Any] = {"since": args.since} if args.since else {}
    limit = "LIMIT :limit" if args.limit else ""
    if args.limit:
        params["limit"] = args.limit

    with engine.connect() as conn:
        filing_rows = conn.execute(
            text(
                f"""
                SELECT fr.accession_number, fr.cik, fr.ticker, fr.filing_type,
                       fr.filing_date, fr.period_of_report
                FROM filings_raw fr
                {where}
                ORDER BY fr.filing_date DESC
                {limit}
                """
            ),
            params,
        ).fetchall()

        if not filing_rows:
            raise SystemExit("No filings matched — nothing to train on.")

        accessions = [r[0] for r in filing_rows]
        fact_rows = conn.execute(
            text(
                """
                SELECT accession_number, fact_name, fact_value, unit,
                       period_start, period_end, segment
                FROM financial_facts
                WHERE accession_number = ANY(:acc)
                """
            ),
            {"acc": accessions},
        ).fetchall()

    filings = {
        r[0]: {
            "accession_number": r[0],
            "cik": r[1],
            "ticker": r[2],
            "filing_type": r[3],
            "filing_date": r[4],
            "period_of_report": r[5],
        }
        for r in filing_rows
    }
    facts = [
        {
            "accession_number": r[0],
            "fact_name": r[1],
            "fact_value": float(r[2]) if r[2] is not None else None,
            "unit": r[3],
            "period_start": r[4],
            "period_end": r[5],
            "segment": r[6],
        }
        for r in fact_rows
    ]

    logger.info("Loaded %d filings / %d facts from the warehouse", len(filings), len(facts))
    return facts, filings


# ---------------------------------------------------------------------------
# Train / evaluate / register
# ---------------------------------------------------------------------------


def _split_holdout(
    facts: list[dict],
    filings: dict[str, dict],
    fraction: float,
    seed: int,
) -> tuple[list[dict], list[dict]]:
    """
    Split by *filing*, never by fact row.

    Splitting rows would put some of a filing's facts in train and the rest in
    test, which leaks the very thing being measured — the model would see the
    same filing on both sides and every metric would be optimistic.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    accessions = sorted({f["accession_number"] for f in facts})
    n_holdout = max(1, round(len(accessions) * fraction))
    holdout = set(rng.choice(accessions, size=min(n_holdout, len(accessions) - 1), replace=False))

    train = [f for f in facts if f["accession_number"] not in holdout]
    test = [f for f in facts if f["accession_number"] in holdout]

    logger.info(
        "Split %d filings: %d train / %d holdout",
        len(accessions),
        len(accessions) - len(holdout),
        len(holdout),
    )
    return train, test


def _save_baseline(registry: Any, version: str, matrix: Any) -> None:
    """
    Store the training feature matrix beside the artifact.

    Drift monitoring needs the distribution the model was fitted on. Keeping it
    next to the model means a drift check can never accidentally compare against
    a different version's baseline.
    """
    import numpy as np

    path = registry.version_dir(version) / "baseline_features.npz"
    np.savez_compressed(
        path,
        X=matrix.X,
        accessions=np.array(matrix.accessions, dtype=object).astype(str),
        feature_names=np.array(list(matrix.feature_names), dtype=str),
    )
    logger.info("Wrote training baseline to %s", path)


def main() -> int:
    args = _parse_args()

    from src.ml.evaluation import evaluate_detector
    from src.ml.features import build_features
    from src.ml.model import AnomalyDetector
    from src.ml.registry import ModelRegistry

    facts, filings = _load_synthetic(args) if args.synthetic else _load_warehouse(args)

    train_facts, holdout_facts = _split_holdout(facts, filings, args.holdout, args.data_seed)

    train_matrix = build_features(train_facts, filings)
    if len(train_matrix) < 2:
        raise SystemExit(f"Need at least 2 training filings, got {len(train_matrix)}")

    detector = AnomalyDetector(
        seed=args.seed,
        contamination=args.contamination,
        n_estimators=args.n_estimators,
        threshold=args.threshold,
    ).fit(train_matrix)

    metrics = evaluate_detector(
        detector,
        holdout_facts,
        filings,
        rate=args.corruption_rate,
        seed=args.eval_seed,
    )

    print()
    print("=" * 72)
    print(f"  Training filings : {len(train_matrix)}")
    print(f"  Holdout filings  : {metrics.n_filings} ({metrics.n_corrupted} corrupted)")
    print(f"  Recall           : {metrics.recall:.3f}")
    print(f"  Precision        : {metrics.precision:.3f}")
    print(f"  F1               : {metrics.f1:.3f}")
    print(f"  ROC AUC          : {metrics.roc_auc:.3f}")
    print(f"  Flag rate        : {metrics.flag_rate:.3f}")
    print("=" * 72)

    if args.metrics_out:
        out = Path(args.metrics_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(metrics.as_dict(), indent=2) + "\n", encoding="utf-8")
        logger.info("Wrote metrics to %s", out)

    if args.no_register:
        logger.info("--no-register set; skipping registry write")
        return 0

    registry = ModelRegistry(args.registry)
    metadata = registry.register(
        detector,
        train_matrix,
        metrics=metrics.as_metrics(),
        notes=args.notes or ("synthetic corpus" if args.synthetic else "warehouse corpus"),
    )
    _save_baseline(registry, metadata.version, train_matrix)

    print()
    print(f"  Registered : {metadata.version}")
    print(f"  Artifact   : sha256:{metadata.artifact_sha256}")
    print(f"  Train data : sha256:{metadata.training_data_sha256}")
    print(f"  Model card : {registry.card_path(metadata.version)}")

    if args.promote:
        registry.promote(metadata.version)
        print(f"  Promoted   : {metadata.version} -> PRODUCTION")
    else:
        print("  Not promoted (pass --promote to serve this version)")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
