"""
Tests for scripts/train_model.py and scripts/evaluate_model.py.

These invoke `main()` directly with monkeypatched ``sys.argv`` rather than
shelling out — faster, and coverage attributes correctly to the script module.
Corpus sizes are kept small (companies/years) since these are integration
tests of the CLI plumbing, not a re-verification of the model's detection
quality (that lives in tests/test_ml_model.py, against a fuller corpus).
"""

from __future__ import annotations

import json

import pytest


def _run_train(monkeypatch, args):
    import scripts.train_model as train_model

    monkeypatch.setattr("sys.argv", ["train_model", *args])
    return train_model.main()


def _run_evaluate(monkeypatch, args):
    import scripts.evaluate_model as evaluate_model

    monkeypatch.setattr("sys.argv", ["evaluate_model", *args])
    return evaluate_model.main()


@pytest.fixture
def registry_root(tmp_path):
    return tmp_path / "models"


# ---------------------------------------------------------------------------
# train_model.py
# ---------------------------------------------------------------------------


class TestTrainModelCLI:
    def test_synthetic_run_registers_a_version(self, monkeypatch, registry_root):
        exit_code = _run_train(
            monkeypatch,
            [
                "--synthetic",
                "--companies",
                "20",
                "--years",
                "3",
                "--registry",
                str(registry_root),
            ],
        )
        assert exit_code == 0

        from src.ml.registry import ModelRegistry

        registry = ModelRegistry(registry_root)
        assert registry.latest_version() is not None
        assert registry.production_version() is None  # --promote not passed

    def test_promote_flag_sets_production(self, monkeypatch, registry_root):
        _run_train(
            monkeypatch,
            [
                "--synthetic",
                "--companies",
                "20",
                "--years",
                "3",
                "--registry",
                str(registry_root),
                "--promote",
            ],
        )

        from src.ml.registry import ModelRegistry

        registry = ModelRegistry(registry_root)
        assert registry.production_version() == registry.latest_version()

    def test_no_register_writes_no_version(self, monkeypatch, registry_root):
        _run_train(
            monkeypatch,
            [
                "--synthetic",
                "--companies",
                "20",
                "--years",
                "3",
                "--registry",
                str(registry_root),
                "--no-register",
            ],
        )

        from src.ml.registry import ModelRegistry

        assert ModelRegistry(registry_root).list_versions() == []

    def test_metrics_out_writes_valid_json(self, monkeypatch, registry_root, tmp_path):
        metrics_path = tmp_path / "metrics.json"
        _run_train(
            monkeypatch,
            [
                "--synthetic",
                "--companies",
                "20",
                "--years",
                "3",
                "--registry",
                str(registry_root),
                "--no-register",
                "--metrics-out",
                str(metrics_path),
            ],
        )

        metrics = json.loads(metrics_path.read_text())
        assert set(metrics) >= {"recall", "precision", "f1", "roc_auc", "flag_rate"}
        assert 0.0 <= metrics["recall"] <= 1.0

    def test_writes_baseline_snapshot_alongside_the_artifact(self, monkeypatch, registry_root):
        _run_train(
            monkeypatch,
            [
                "--synthetic",
                "--companies",
                "20",
                "--years",
                "3",
                "--registry",
                str(registry_root),
            ],
        )

        from src.ml.registry import ModelRegistry

        registry = ModelRegistry(registry_root)
        version = registry.latest_version()
        assert (registry.version_dir(version) / "baseline_features.npz").exists()

    def test_identical_flags_reproduce_the_same_artifact_hash(self, monkeypatch, tmp_path):
        from src.ml.registry import ModelRegistry

        common_args = [
            "--synthetic",
            "--companies",
            "20",
            "--years",
            "3",
            "--seed",
            "42",
            "--data-seed",
            "7",
        ]

        root_a = tmp_path / "a"
        _run_train(monkeypatch, [*common_args, "--registry", str(root_a)])
        root_b = tmp_path / "b"
        _run_train(monkeypatch, [*common_args, "--registry", str(root_b)])

        meta_a = ModelRegistry(root_a).get_metadata(ModelRegistry(root_a).latest_version())
        meta_b = ModelRegistry(root_b).get_metadata(ModelRegistry(root_b).latest_version())
        assert meta_a.artifact_sha256 == meta_b.artifact_sha256

    def test_warehouse_mode_without_database_url_exits_cleanly(self, monkeypatch, registry_root):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(SystemExit):
            _run_train(monkeypatch, ["--registry", str(registry_root)])

    def test_notes_are_recorded_in_metadata(self, monkeypatch, registry_root):
        _run_train(
            monkeypatch,
            [
                "--synthetic",
                "--companies",
                "20",
                "--years",
                "3",
                "--registry",
                str(registry_root),
                "--notes",
                "unit test run",
            ],
        )
        from src.ml.registry import ModelRegistry

        registry = ModelRegistry(registry_root)
        metadata = registry.get_metadata(registry.latest_version())
        assert metadata.notes == "unit test run"


class TestSplitHoldout:
    def test_splits_by_filing_not_by_row(self):
        from scripts.train_model import _split_holdout

        facts = [
            {"accession_number": "A", "fact_name": "us-gaap:Revenues", "fact_value": 1.0},
            {"accession_number": "A", "fact_name": "us-gaap:Assets", "fact_value": 2.0},
            {"accession_number": "B", "fact_name": "us-gaap:Revenues", "fact_value": 3.0},
        ]
        train, holdout = _split_holdout(facts, {}, fraction=0.5, seed=0)

        train_accessions = {f["accession_number"] for f in train}
        holdout_accessions = {f["accession_number"] for f in holdout}
        # No filing appears on both sides — that would leak the holdout signal.
        assert train_accessions.isdisjoint(holdout_accessions)

    def test_holdout_fraction_is_approximately_respected(self):
        from scripts.train_model import _split_holdout

        facts = [
            {"accession_number": f"A{i}", "fact_name": "us-gaap:Revenues", "fact_value": 1.0}
            for i in range(100)
        ]
        _, holdout = _split_holdout(facts, {}, fraction=0.2, seed=0)
        holdout_accessions = {f["accession_number"] for f in holdout}
        assert 15 <= len(holdout_accessions) <= 25


# ---------------------------------------------------------------------------
# evaluate_model.py
# ---------------------------------------------------------------------------


class TestEvaluateModelCLI:
    def _register_model(self, monkeypatch, registry_root, metrics_path):
        _run_train(
            monkeypatch,
            [
                "--synthetic",
                "--companies",
                "20",
                "--years",
                "3",
                "--registry",
                str(registry_root),
                "--promote",
                "--metrics-out",
                str(metrics_path),
            ],
        )

    def test_passes_when_floors_are_met(self, monkeypatch, registry_root, tmp_path):
        metrics_path = tmp_path / "metrics.json"
        self._register_model(monkeypatch, registry_root, metrics_path)

        exit_code = _run_evaluate(
            monkeypatch,
            [
                "--metrics",
                str(metrics_path),
                "--registry",
                str(registry_root),
                "--min-recall",
                "0.0",
                "--min-precision",
                "0.0",
                "--min-roc-auc",
                "0.0",
            ],
        )
        assert exit_code == 0

    def test_fails_when_a_floor_is_not_met(self, monkeypatch, registry_root, tmp_path):
        metrics_path = tmp_path / "metrics.json"
        self._register_model(monkeypatch, registry_root, metrics_path)

        exit_code = _run_evaluate(
            monkeypatch,
            [
                "--metrics",
                str(metrics_path),
                "--registry",
                str(registry_root),
                "--min-recall",
                "1.1",  # unattainable
            ],
        )
        assert exit_code == 1

    def test_missing_metrics_file_exits_2(self, monkeypatch, tmp_path):
        exit_code = _run_evaluate(monkeypatch, ["--metrics", str(tmp_path / "nope.json")])
        assert exit_code == 2

    def test_neither_metrics_nor_version_exits(self, monkeypatch):
        with pytest.raises(SystemExit):
            _run_evaluate(monkeypatch, [])

    def test_verify_only_on_intact_artifact_passes(self, monkeypatch, registry_root, tmp_path):
        self._register_model(monkeypatch, registry_root, tmp_path / "metrics.json")
        exit_code = _run_evaluate(
            monkeypatch,
            ["--verify-only", "--version", "latest", "--registry", str(registry_root)],
        )
        assert exit_code == 0

    def test_verify_only_on_tampered_artifact_fails(self, monkeypatch, registry_root, tmp_path):
        self._register_model(monkeypatch, registry_root, tmp_path / "metrics.json")

        from src.ml.registry import ModelRegistry

        registry = ModelRegistry(registry_root)
        version = registry.latest_version()
        artifact = registry.artifact_path(version)
        artifact.write_bytes(artifact.read_bytes() + b"tampered")

        exit_code = _run_evaluate(
            monkeypatch,
            ["--verify-only", "--version", "latest", "--registry", str(registry_root)],
        )
        assert exit_code == 1

    def test_regression_against_baseline_fails_on_a_worse_candidate(
        self, monkeypatch, registry_root, tmp_path
    ):
        baseline_metrics = tmp_path / "baseline.json"
        self._register_model(monkeypatch, registry_root, baseline_metrics)

        degraded = tmp_path / "degraded.json"
        original = json.loads(baseline_metrics.read_text())
        degraded.write_text(
            json.dumps({**original, "recall": 0.0, "precision": 0.0, "roc_auc": 0.5})
        )

        exit_code = _run_evaluate(
            monkeypatch,
            [
                "--metrics",
                str(degraded),
                "--registry",
                str(registry_root),
                "--min-recall",
                "0.0",
                "--min-precision",
                "0.0",
                "--min-roc-auc",
                "0.0",
            ],
        )
        assert exit_code == 1

    def test_skip_regression_ignores_the_baseline(self, monkeypatch, registry_root, tmp_path):
        baseline_metrics = tmp_path / "baseline.json"
        self._register_model(monkeypatch, registry_root, baseline_metrics)

        degraded = tmp_path / "degraded.json"
        original = json.loads(baseline_metrics.read_text())
        degraded.write_text(
            json.dumps({**original, "recall": 0.0, "precision": 0.0, "roc_auc": 0.5})
        )

        exit_code = _run_evaluate(
            monkeypatch,
            [
                "--metrics",
                str(degraded),
                "--registry",
                str(registry_root),
                "--min-recall",
                "0.0",
                "--min-precision",
                "0.0",
                "--min-roc-auc",
                "0.0",
                "--skip-regression",
            ],
        )
        assert exit_code == 0

    def test_first_model_has_no_baseline_and_still_passes(self, monkeypatch, tmp_path):
        """No promoted model yet — the regression check must not block bootstrapping."""
        registry_root = tmp_path / "empty-registry"
        metrics_path = tmp_path / "metrics.json"
        metrics_path.write_text(
            json.dumps({"recall": 0.8, "precision": 0.6, "roc_auc": 0.9, "flag_rate": 0.1})
        )

        exit_code = _run_evaluate(
            monkeypatch, ["--metrics", str(metrics_path), "--registry", str(registry_root)]
        )
        assert exit_code == 0

    def test_high_flag_rate_fails_even_with_good_metrics(self, monkeypatch, tmp_path):
        registry_root = tmp_path / "empty-registry"
        metrics_path = tmp_path / "metrics.json"
        metrics_path.write_text(
            json.dumps({"recall": 0.9, "precision": 0.9, "roc_auc": 0.95, "flag_rate": 0.9})
        )

        exit_code = _run_evaluate(
            monkeypatch, ["--metrics", str(metrics_path), "--registry", str(registry_root)]
        )
        assert exit_code == 1

    def test_summary_out_writes_markdown(self, monkeypatch, registry_root, tmp_path):
        metrics_path = tmp_path / "metrics.json"
        self._register_model(monkeypatch, registry_root, metrics_path)
        summary_path = tmp_path / "summary.md"

        _run_evaluate(
            monkeypatch,
            [
                "--metrics",
                str(metrics_path),
                "--registry",
                str(registry_root),
                "--min-recall",
                "0.0",
                "--min-precision",
                "0.0",
                "--min-roc-auc",
                "0.0",
                "--summary-out",
                str(summary_path),
            ],
        )
        content = summary_path.read_text()
        assert "Model gate" in content
        assert "|" in content  # a markdown table was written
