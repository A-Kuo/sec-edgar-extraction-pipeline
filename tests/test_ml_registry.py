"""
Tests for src/ml/registry.py.

The registry's job is provenance, so the tests that matter are the ones about
identity and tampering: does the recorded hash actually match the artifact, does
verification catch a file that changed after registration, and does promotion
refuse to point PRODUCTION at something unverifiable.
"""

from __future__ import annotations

import json

import pytest

from src.ml.model import AnomalyDetector
from src.ml.registry import (
    ModelMetadata,
    ModelNotFoundError,
    ModelRegistry,
    hash_training_data,
    render_model_card,
    sha256_file,
)


@pytest.fixture
def registry(tmp_path):
    return ModelRegistry(tmp_path / "models")


@pytest.fixture
def registered(registry, fitted_detector, feature_matrix):
    """A registry containing one registered version."""
    metadata = registry.register(
        fitted_detector, feature_matrix, metrics={"recall": 0.8, "roc_auc": 0.95}
    )
    return registry, metadata


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------


class TestHashing:
    def test_file_hash_is_stable(self, tmp_path):
        path = tmp_path / "f.bin"
        path.write_bytes(b"contents")
        assert sha256_file(path) == sha256_file(path)

    def test_file_hash_changes_with_contents(self, tmp_path):
        path = tmp_path / "f.bin"
        path.write_bytes(b"a")
        first = sha256_file(path)
        path.write_bytes(b"b")
        assert sha256_file(path) != first

    def test_training_data_hash_is_stable(self, feature_matrix):
        assert hash_training_data(feature_matrix) == hash_training_data(feature_matrix)

    def test_training_data_hash_reflects_the_values(self, feature_matrix):
        mutated = type(feature_matrix)(
            X=feature_matrix.X + 1.0,
            accessions=feature_matrix.accessions,
            feature_names=feature_matrix.feature_names,
        )
        assert hash_training_data(mutated) != hash_training_data(feature_matrix)

    def test_training_data_hash_reflects_the_row_labels(self, feature_matrix):
        relabelled = type(feature_matrix)(
            X=feature_matrix.X,
            accessions=[f"X{a}" for a in feature_matrix.accessions],
            feature_names=feature_matrix.feature_names,
        )
        assert hash_training_data(relabelled) != hash_training_data(feature_matrix)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_refuses_an_unfitted_detector(self, registry, feature_matrix):
        with pytest.raises(ValueError, match="unfitted"):
            registry.register(AnomalyDetector(), feature_matrix)

    def test_writes_artifact_metadata_and_card(self, registered):
        registry, metadata = registered
        assert registry.artifact_path(metadata.version).exists()
        assert registry.metadata_path(metadata.version).exists()
        assert registry.card_path(metadata.version).exists()

    def test_version_embeds_the_artifact_hash(self, registered):
        _, metadata = registered
        assert metadata.version.endswith(metadata.artifact_sha256[:8])

    def test_recorded_hash_matches_the_file_on_disk(self, registered):
        registry, metadata = registered
        assert sha256_file(registry.artifact_path(metadata.version)) == metadata.artifact_sha256

    def test_records_provenance_fields(self, registered, feature_matrix):
        _, metadata = registered
        assert metadata.n_training_filings == len(feature_matrix)
        assert metadata.seed == 42
        assert metadata.library_versions["scikit-learn"]
        assert metadata.training_data_sha256

    def test_records_metrics(self, registered):
        _, metadata = registered
        assert metadata.metrics == {"recall": 0.8, "roc_auc": 0.95}

    def test_metadata_round_trips_through_json(self, registered):
        _, metadata = registered
        assert ModelMetadata.from_json(metadata.to_json()) == metadata

    def test_staging_directory_is_cleaned_up(self, registered):
        registry, _ = registered
        assert not (registry.root / ".staging").exists()

    def test_registering_twice_creates_two_versions(
        self, registry, fitted_detector, feature_matrix
    ):
        first = registry.register(fitted_detector, feature_matrix)
        second = registry.register(fitted_detector, feature_matrix)
        # Same artifact hash (training is deterministic), distinct versions
        # because the timestamp differs.
        assert first.artifact_sha256 == second.artifact_sha256
        assert len(registry.list_versions()) == 2


# ---------------------------------------------------------------------------
# Listing and loading
# ---------------------------------------------------------------------------


class TestListingAndLoading:
    def test_empty_registry_lists_nothing(self, registry):
        assert registry.list_versions() == []
        assert registry.latest_version() is None
        assert registry.production_version() is None

    def test_loading_from_an_empty_registry_raises(self, registry):
        with pytest.raises(ModelNotFoundError):
            registry.load()

    def test_load_returns_a_working_detector(self, registered, feature_matrix):
        registry, metadata = registered
        detector, loaded_metadata = registry.load(metadata.version)
        assert loaded_metadata.version == metadata.version
        assert len(detector.score(feature_matrix)) == len(feature_matrix)

    def test_load_defaults_to_latest(self, registered):
        registry, metadata = registered
        _, loaded = registry.load()
        assert loaded.version == metadata.version

    def test_loaded_model_reproduces_original_scores(
        self, registered, fitted_detector, feature_matrix
    ):
        registry, metadata = registered
        loaded, _ = registry.load(metadata.version)
        assert [s.score for s in loaded.score(feature_matrix)] == [
            s.score for s in fitted_detector.score(feature_matrix)
        ]

    def test_unknown_version_raises(self, registered):
        registry, _ = registered
        with pytest.raises(ModelNotFoundError):
            registry.get_metadata("v-does-not-exist")


# ---------------------------------------------------------------------------
# Verification — the tamper check
# ---------------------------------------------------------------------------


class TestVerification:
    def test_intact_artifact_verifies(self, registered):
        registry, metadata = registered
        assert registry.verify(metadata.version) is True

    def test_modified_artifact_fails_verification(self, registered):
        registry, metadata = registered
        artifact = registry.artifact_path(metadata.version)
        artifact.write_bytes(artifact.read_bytes() + b"tampered")

        with pytest.raises(ValueError, match="hash mismatch"):
            registry.verify(metadata.version)

    def test_missing_artifact_fails_verification(self, registered):
        registry, metadata = registered
        registry.artifact_path(metadata.version).unlink()

        with pytest.raises(ModelNotFoundError):
            registry.verify(metadata.version)

    def test_load_verifies_before_returning(self, registered):
        """A tampered model must not be loadable, not merely detectable."""
        registry, metadata = registered
        artifact = registry.artifact_path(metadata.version)
        artifact.write_bytes(artifact.read_bytes() + b"tampered")

        with pytest.raises(ValueError, match="hash mismatch"):
            registry.load(metadata.version)


# ---------------------------------------------------------------------------
# Promotion
# ---------------------------------------------------------------------------


class TestPromotion:
    def test_promote_sets_the_pointer(self, registered):
        registry, metadata = registered
        registry.promote(metadata.version)
        assert registry.production_version() == metadata.version

    def test_promoting_an_unknown_version_raises(self, registered):
        registry, _ = registered
        with pytest.raises(ModelNotFoundError):
            registry.promote("v-nope")

    def test_promoting_a_tampered_version_raises(self, registered):
        registry, metadata = registered
        artifact = registry.artifact_path(metadata.version)
        artifact.write_bytes(artifact.read_bytes() + b"tampered")

        with pytest.raises(ValueError, match="hash mismatch"):
            registry.promote(metadata.version)

    def test_load_production_before_promotion_raises(self, registered):
        registry, _ = registered
        with pytest.raises(ModelNotFoundError, match="promoted"):
            registry.load_production()

    def test_load_production_returns_the_promoted_version(self, registered):
        registry, metadata = registered
        registry.promote(metadata.version)
        _, loaded = registry.load_production()
        assert loaded.version == metadata.version

    def test_promotion_can_be_moved(self, registry, fitted_detector, feature_matrix):
        first = registry.register(fitted_detector, feature_matrix)
        second = registry.register(fitted_detector, feature_matrix)

        registry.promote(first.version)
        assert registry.production_version() == first.version

        registry.promote(second.version)
        assert registry.production_version() == second.version


# ---------------------------------------------------------------------------
# Model card
# ---------------------------------------------------------------------------


class TestModelCard:
    def test_card_states_intended_use_and_limits(self, registered):
        registry, metadata = registered
        card = registry.card_path(metadata.version).read_text()
        assert "## Intended use" in card
        assert "## Out of scope" in card
        assert "## Limitations" in card

    def test_card_carries_the_provenance_hashes(self, registered):
        registry, metadata = registered
        card = registry.card_path(metadata.version).read_text()
        assert metadata.artifact_sha256 in card
        assert metadata.training_data_sha256 in card

    def test_card_includes_reproduction_instructions(self, registered):
        registry, metadata = registered
        card = registry.card_path(metadata.version).read_text()
        assert "scripts/train_model.py" in card
        assert f"--seed {metadata.seed}" in card

    def test_card_renders_metrics(self, registered):
        registry, metadata = registered
        card = registry.card_path(metadata.version).read_text()
        assert "`recall`" in card

    def test_card_handles_absent_metrics(self):
        metadata = ModelMetadata(
            version="v1",
            trained_at="2026-01-01T00:00:00Z",
            artifact_sha256="a" * 64,
            training_data_sha256="b" * 64,
            n_training_filings=10,
            feature_names=["f1"],
            seed=1,
            contamination=0.05,
            n_estimators=10,
            threshold=0.6,
        )
        assert "_none recorded_" in render_model_card(metadata)


# ---------------------------------------------------------------------------
# Baseline snapshot (written by scripts/train_model.py, read by the DAG)
# ---------------------------------------------------------------------------


class TestBaselineSnapshot:
    def test_baseline_round_trips(self, registered, feature_matrix, tmp_path):
        import numpy as np

        from scripts.train_model import _save_baseline
        from src.ml.features import FeatureMatrix

        registry, metadata = registered
        _save_baseline(registry, metadata.version, feature_matrix)

        path = registry.version_dir(metadata.version) / "baseline_features.npz"
        assert path.exists()

        stored = np.load(path, allow_pickle=False)
        restored = FeatureMatrix(
            X=stored["X"],
            accessions=[str(a) for a in stored["accessions"]],
            feature_names=tuple(str(n) for n in stored["feature_names"]),
        )
        assert restored.accessions == feature_matrix.accessions
        assert restored.feature_names == feature_matrix.feature_names
        np.testing.assert_allclose(restored.X, feature_matrix.X)


# ---------------------------------------------------------------------------
# Metadata on disk
# ---------------------------------------------------------------------------


def test_metadata_json_is_human_readable(registered):
    """Metadata is an audit record; it has to be greppable without tooling."""
    registry, metadata = registered
    raw = registry.metadata_path(metadata.version).read_text()
    parsed = json.loads(raw)

    assert parsed["version"] == metadata.version
    assert parsed["artifact_sha256"] == metadata.artifact_sha256
    assert "\n" in raw  # indented, not a single line
