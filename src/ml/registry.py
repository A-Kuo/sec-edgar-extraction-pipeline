"""
Content-addressed model registry with provenance metadata.

The rest of this pipeline can answer "where did this number come from?" for any
extracted fact. A model that scores those facts has to meet the same bar, so
the registry records, for every version:

* the SHA-256 of the artifact itself — proof the file scoring today is the file
  that was evaluated,
* the SHA-256 of the exact feature matrix it was trained on — proof of which
  filings shaped it,
* the seed, hyperparameters, library versions and git commit — enough to
  rebuild it from scratch,
* the evaluation metrics it was promoted on.

Layout on disk::

    models/
      PRODUCTION                  # one line: the promoted version string
      v20260811T142530Z-a1b2c3d4/
        model.joblib
        metadata.json
        MODEL_CARD.md

Versions are ordered lexicographically by their UTC timestamp prefix, so
``latest`` needs no index file and the directory listing is self-describing.
The trailing hash makes two models trained in the same second distinguishable.
"""

from __future__ import annotations

import hashlib
import json
import logging
import platform
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

from src.ml.features import FeatureMatrix
from src.ml.model import AnomalyDetector

logger = logging.getLogger(__name__)

DEFAULT_REGISTRY_ROOT = Path("models")
PRODUCTION_POINTER = "PRODUCTION"
_ARTIFACT_NAME = "model.joblib"
_METADATA_NAME = "metadata.json"
_CARD_NAME = "MODEL_CARD.md"


# ---------------------------------------------------------------------------
# Hashing helpers
# ---------------------------------------------------------------------------


def sha256_file(path: str | Path, *, chunk_size: int = 1 << 20) -> str:
    """Stream a file through SHA-256 so large artifacts don't load into memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_training_data(matrix: FeatureMatrix) -> str:
    """
    Fingerprint the exact training set.

    Covers the accession list, the feature names, and the feature values
    themselves. Rounding to 6 decimals before hashing keeps the fingerprint
    stable across platforms whose floating-point formatting differs in the last
    bit, without being loose enough to hide a real data change.
    """
    digest = hashlib.sha256()
    digest.update("|".join(matrix.accessions).encode("utf-8"))
    digest.update(b"\x00")
    digest.update("|".join(matrix.feature_names).encode("utf-8"))
    digest.update(b"\x00")
    digest.update(np.round(matrix.X, 6).tobytes())
    return digest.hexdigest()


def _git_sha() -> str:
    """Current commit, or ``"unknown"`` outside a git checkout."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def _library_versions() -> dict[str, str]:
    import sklearn

    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "scikit-learn": sklearn.__version__,
    }


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


@dataclass
class ModelMetadata:
    """Everything needed to identify, reproduce, and audit one model version."""

    version: str
    trained_at: str
    artifact_sha256: str
    training_data_sha256: str
    n_training_filings: int
    feature_names: list[str]
    seed: int
    contamination: float
    n_estimators: int
    threshold: float
    git_sha: str = "unknown"
    library_versions: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    notes: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, raw: str) -> ModelMetadata:
        return cls(**json.loads(raw))


class ModelNotFoundError(FileNotFoundError):
    """Raised when a requested model version is absent from the registry."""


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class ModelRegistry:
    """
    Filesystem-backed registry of versioned anomaly detectors.

    A local directory is deliberate: it works identically in a CI runner, a
    container, and on a laptop, and it maps cleanly onto object storage when the
    time comes (the layout is a plain prefix tree). Nothing here depends on a
    running service, so a scoring job cannot be blocked by a registry outage.

    Example::

        registry = ModelRegistry("models")
        meta = registry.register(detector, matrix, metrics={"recall_at_5pct": 0.92})
        registry.promote(meta.version)
        production, production_meta = registry.load_production()
    """

    def __init__(self, root: str | Path = DEFAULT_REGISTRY_ROOT) -> None:
        self.root = Path(root)

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    def version_dir(self, version: str) -> Path:
        return self.root / version

    def artifact_path(self, version: str) -> Path:
        return self.version_dir(version) / _ARTIFACT_NAME

    def metadata_path(self, version: str) -> Path:
        return self.version_dir(version) / _METADATA_NAME

    def card_path(self, version: str) -> Path:
        return self.version_dir(version) / _CARD_NAME

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    def list_versions(self) -> list[str]:
        """Registered versions, oldest first."""
        if not self.root.exists():
            return []
        return sorted(
            entry.name
            for entry in self.root.iterdir()
            if entry.is_dir() and (entry / _METADATA_NAME).exists()
        )

    def latest_version(self) -> str | None:
        versions = self.list_versions()
        return versions[-1] if versions else None

    def production_version(self) -> str | None:
        """The promoted version, or None if nothing has been promoted."""
        pointer = self.root / PRODUCTION_POINTER
        if not pointer.exists():
            return None
        version = pointer.read_text(encoding="utf-8").strip()
        return version or None

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def _make_version(self, artifact_sha: str) -> str:
        """
        Build a unique, lexicographically-ordered version string.

        The hash suffix identifies the contents, but it cannot disambiguate:
        training is deterministic by design, so registering the same model twice
        yields the *same* hash. Two registrations inside one second therefore
        collided on a seconds-resolution timestamp. Milliseconds make that
        vanishingly unlikely, and the numeric suffix closes it completely rather
        than leaving a race that only shows up under a fast CI runner.
        """
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")[:-4]
        base = f"v{stamp}-{artifact_sha[:8]}"

        version = base
        suffix = 1
        while self.version_dir(version).exists():
            suffix += 1
            version = f"{base}.{suffix}"
        return version

    def register(
        self,
        detector: AnomalyDetector,
        training_matrix: FeatureMatrix,
        *,
        metrics: dict[str, float] | None = None,
        notes: str = "",
    ) -> ModelMetadata:
        """
        Persist *detector* as a new immutable version and return its metadata.

        The artifact is written to a temporary location first so its hash can
        become part of the version string — the directory name therefore
        contains a fingerprint of its own contents.
        """
        if not detector.is_fitted:
            raise ValueError("Refusing to register an unfitted detector")

        self.root.mkdir(parents=True, exist_ok=True)
        staging = self.root / ".staging"
        staging.mkdir(parents=True, exist_ok=True)
        staged_artifact = staging / _ARTIFACT_NAME

        try:
            detector.save(staged_artifact)
            artifact_sha = sha256_file(staged_artifact)
            version = self._make_version(artifact_sha)

            target_dir = self.version_dir(version)
            if target_dir.exists():
                raise FileExistsError(f"Version {version} already registered")
            target_dir.mkdir(parents=True)
            staged_artifact.replace(target_dir / _ARTIFACT_NAME)
        finally:
            if staged_artifact.exists():
                staged_artifact.unlink()
            if staging.exists() and not any(staging.iterdir()):
                staging.rmdir()

        metadata = ModelMetadata(
            version=version,
            trained_at=datetime.now(UTC).isoformat(),
            artifact_sha256=artifact_sha,
            training_data_sha256=hash_training_data(training_matrix),
            n_training_filings=len(training_matrix),
            feature_names=list(detector.feature_names),
            seed=detector.seed,
            contamination=detector.contamination,
            n_estimators=detector.n_estimators,
            threshold=detector.threshold,
            git_sha=_git_sha(),
            library_versions=_library_versions(),
            metrics=dict(metrics or {}),
            notes=notes,
        )
        self.metadata_path(version).write_text(metadata.to_json(), encoding="utf-8")
        self.card_path(version).write_text(render_model_card(metadata), encoding="utf-8")

        logger.info(
            "Registered model %s (artifact sha256=%s, %d training filings)",
            version,
            artifact_sha[:12],
            metadata.n_training_filings,
        )
        return metadata

    def promote(self, version: str) -> None:
        """Point PRODUCTION at *version* after verifying it is intact."""
        if version not in self.list_versions():
            raise ModelNotFoundError(f"Cannot promote unknown version {version!r}")
        self.verify(version)
        (self.root / PRODUCTION_POINTER).write_text(version + "\n", encoding="utf-8")
        logger.info("Promoted %s to production", version)

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------

    def get_metadata(self, version: str) -> ModelMetadata:
        path = self.metadata_path(version)
        if not path.exists():
            raise ModelNotFoundError(f"No metadata for version {version!r} in {self.root}")
        return ModelMetadata.from_json(path.read_text(encoding="utf-8"))

    def verify(self, version: str) -> bool:
        """
        Re-hash the stored artifact and compare against its recorded digest.

        This is the tamper check: a model swapped on disk after registration
        fails here rather than silently scoring production traffic.
        """
        metadata = self.get_metadata(version)
        artifact = self.artifact_path(version)
        if not artifact.exists():
            raise ModelNotFoundError(f"Missing artifact for version {version!r}")

        actual = sha256_file(artifact)
        if actual != metadata.artifact_sha256:
            raise ValueError(
                f"Artifact hash mismatch for {version}:\n"
                f"  recorded: {metadata.artifact_sha256}\n"
                f"  actual:   {actual}\n"
                "The model file has changed since registration."
            )
        return True

    def load(self, version: str | None = None) -> tuple[AnomalyDetector, ModelMetadata]:
        """
        Load a version (default: latest), verifying its hash first.

        Returns ``(detector, metadata)`` so callers can stamp the model version
        onto every score they write.
        """
        resolved = version or self.latest_version()
        if resolved is None:
            raise ModelNotFoundError(f"No models registered in {self.root}")
        self.verify(resolved)
        return AnomalyDetector.load(self.artifact_path(resolved)), self.get_metadata(resolved)

    def load_production(self) -> tuple[AnomalyDetector, ModelMetadata]:
        """Load the promoted model. Raises if nothing has been promoted."""
        version = self.production_version()
        if version is None:
            raise ModelNotFoundError(
                f"No production model promoted in {self.root}. "
                "Run `python scripts/train_model.py --promote` first."
            )
        return self.load(version)


# ---------------------------------------------------------------------------
# Model card
# ---------------------------------------------------------------------------


def render_model_card(metadata: ModelMetadata) -> str:
    """
    Render a Model Card for *metadata*.

    Following the Model Cards convention (Mitchell et al., 2019): state what the
    model is for, what it is explicitly not for, and how it was measured — so a
    reviewer who did not build it can judge whether a given use is appropriate.
    """
    metrics_rows = (
        "\n".join(f"| `{k}` | {v:.4f} |" for k, v in sorted(metadata.metrics.items()))
        or "| _none recorded_ | — |"
    )
    libs = ", ".join(f"{k} {v}" for k, v in sorted(metadata.library_versions.items()))

    return f"""# Model Card — `{metadata.version}`

## Overview

| Field | Value |
|---|---|
| Version | `{metadata.version}` |
| Trained at | {metadata.trained_at} |
| Git commit | `{metadata.git_sha}` |
| Artifact SHA-256 | `{metadata.artifact_sha256}` |
| Training data SHA-256 | `{metadata.training_data_sha256}` |
| Training filings | {metadata.n_training_filings} |
| Libraries | {libs} |

## Intended use

Flags SEC 10-K/10-Q filings whose **extracted** financial facts are internally
inconsistent or implausible, producing a ranked review queue for data-quality
triage.

## Out of scope

- **Not** an extraction step. Scores never modify a filed value; XBRL parsing
  stays deterministic and remains the sole source of reported numbers.
- **Not** a fraud or misstatement detector. A flag means "these extracted
  numbers do not hang together", which is most often a parsing defect, not an
  assertion about the filer.
- **Not** an investment signal.

## Architecture

`StandardScaler` → `IsolationForest`

| Hyperparameter | Value |
|---|---|
| `seed` | {metadata.seed} |
| `contamination` | {metadata.contamination} |
| `n_estimators` | {metadata.n_estimators} |
| `threshold` | {metadata.threshold} |

Features ({len(metadata.feature_names)}): {", ".join(f"`{f}`" for f in metadata.feature_names)}

## Evaluation

Measured by injecting known corruptions (scale errors, sign flips, dropped
facts) into held-out filings and checking whether the detector surfaces them.
Labelled extraction errors do not exist naturally at useful volume, so synthetic
injection is the only honest way to attach a recall number to this model.

| Metric | Value |
|---|---|
{metrics_rows}

## Limitations

- `contamination` is an assumption, not a learned quantity. It sets the flag
  rate directly; the evaluation measures recall *at* that rate.
- Continuity features are neutral-filled for a company's first filing, so
  first-time filers are scored on cross-sectional evidence alone.
- The model is fitted on the companies in its training set. Applying it to a
  materially different population (a new sector, a different filing type) needs
  a drift check first — see `src/ml/monitoring.py`.

## Reproduction

```bash
git checkout {metadata.git_sha}
python scripts/train_model.py --seed {metadata.seed} \\
    --contamination {metadata.contamination} \\
    --n-estimators {metadata.n_estimators}
```

Verify the result matches this version:

```bash
python -c "from src.ml.registry import ModelRegistry; \\
    ModelRegistry('models').verify('{metadata.version}')"
```
"""


def load_metadata_dict(path: str | Path) -> dict[str, Any]:
    """Read a metadata.json as a plain dict, for CI steps that only need fields."""
    return json.loads(Path(path).read_text(encoding="utf-8"))
