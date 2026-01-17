"""Tests for stage lineage queries.

These tests verify we can answer "which preprocessing version is my model using?"
"""

from datetime import UTC, datetime

from goldfish.db.database import Database
from goldfish.state_machine import EventContext, StageEvent, transition


def _transition_to_completed(db: Database, stage_run_id: str) -> None:
    """Transition a stage run to COMPLETED state via state machine (v1.2 lifecycle)."""
    ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
    transition(db, stage_run_id, StageEvent.BUILD_START, ctx)
    transition(db, stage_run_id, StageEvent.BUILD_OK, ctx)
    transition(db, stage_run_id, StageEvent.LAUNCH_OK, ctx)
    success_ctx = EventContext(timestamp=datetime.now(UTC), source="executor", exit_code=0, exit_code_exists=True)
    transition(db, stage_run_id, StageEvent.EXIT_SUCCESS, success_ctx)
    transition(db, stage_run_id, StageEvent.POST_RUN_OK, ctx)
    # v1.2: Now need USER_FINALIZE to reach COMPLETED
    finalize_ctx = EventContext(timestamp=datetime.now(UTC), source="mcp_tool")
    transition(db, stage_run_id, StageEvent.USER_FINALIZE, finalize_ctx)


def _transition_to_failed(db: Database, stage_run_id: str) -> None:
    """Transition a stage run to FAILED state via state machine."""
    ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
    transition(db, stage_run_id, StageEvent.BUILD_START, ctx)
    transition(db, stage_run_id, StageEvent.BUILD_OK, ctx)
    transition(db, stage_run_id, StageEvent.LAUNCH_OK, ctx)
    fail_ctx = EventContext(timestamp=datetime.now(UTC), source="executor", exit_code=1, exit_code_exists=True)
    transition(db, stage_run_id, StageEvent.EXIT_FAILURE, fail_ctx)


class TestLineageTreeQueries:
    """Tests for recursive lineage tree building."""

    def test_get_lineage_tree_single_stage(self, test_db):
        """Simple lineage: one stage with no upstream dependencies."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Create stage version and run
        sv_id, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)
        test_db.create_stage_run(
            stage_run_id="stage-001",
            workspace_name="test-ws",
            version="v1",
            stage_name="preprocess",
        )
        test_db.update_stage_run_version("stage-001", sv_id)
        _transition_to_completed(test_db, "stage-001")

        # Query lineage
        lineage = test_db.get_lineage_tree("stage-001")

        assert lineage is not None
        assert lineage["run_id"] == "stage-001"
        assert lineage["stage"] == "preprocess"
        assert lineage["stage_version_num"] == 1
        assert lineage["git_sha"] == "sha1"
        assert lineage["inputs"] == {}  # No upstream inputs

    def test_get_lineage_tree_with_upstream_stage(self, test_db):
        """Lineage with one upstream stage dependency."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Create preprocess stage version and run
        sv_preprocess, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)
        test_db.create_stage_run(
            stage_run_id="stage-preprocess",
            workspace_name="test-ws",
            version="v1",
            stage_name="preprocess",
        )
        test_db.update_stage_run_version("stage-preprocess", sv_preprocess)
        _transition_to_completed(test_db, "stage-preprocess")
        test_db.add_signal(
            stage_run_id="stage-preprocess",
            signal_name="features",
            signal_type="npy",
            storage_location="gs://bucket/features.npy",
        )

        # Create train stage that depends on preprocess
        sv_train, _, _ = test_db.get_or_create_stage_version("test-ws", "train", "sha1", "b" * 64)
        test_db.create_stage_run(
            stage_run_id="stage-train",
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )
        test_db.update_stage_run_version("stage-train", sv_train)
        _transition_to_completed(test_db, "stage-train")

        # Record that train's "features" input came from preprocess
        test_db.add_signal_with_source(
            stage_run_id="stage-train",
            signal_name="features",
            signal_type="input",
            storage_location="gs://bucket/features.npy",
            source_stage_run_id="stage-preprocess",
            source_stage_version_id=sv_preprocess,
        )

        # Query train's lineage
        lineage = test_db.get_lineage_tree("stage-train")

        assert lineage["run_id"] == "stage-train"
        assert lineage["stage"] == "train"
        assert "features" in lineage["inputs"]

        features_input = lineage["inputs"]["features"]
        assert features_input["source_type"] == "stage"
        assert features_input["source_stage_run_id"] == "stage-preprocess"
        assert features_input["source_stage_version_num"] == 1

    def test_get_lineage_tree_multi_level(self, test_db):
        """Deep lineage: A -> B -> C (3 levels)."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Level 1: Preprocess
        sv_preprocess, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)
        test_db.create_stage_run(
            stage_run_id="stage-preprocess",
            workspace_name="test-ws",
            version="v1",
            stage_name="preprocess",
        )
        test_db.update_stage_run_version("stage-preprocess", sv_preprocess)
        _transition_to_completed(test_db, "stage-preprocess")
        test_db.add_signal("stage-preprocess", "features", "npy", "gs://bucket/features.npy")

        # Level 2: Tokenize (depends on preprocess)
        sv_tokenize, _, _ = test_db.get_or_create_stage_version("test-ws", "tokenize", "sha1", "b" * 64)
        test_db.create_stage_run(
            stage_run_id="stage-tokenize",
            workspace_name="test-ws",
            version="v1",
            stage_name="tokenize",
        )
        test_db.update_stage_run_version("stage-tokenize", sv_tokenize)
        _transition_to_completed(test_db, "stage-tokenize")
        test_db.add_signal_with_source(
            "stage-tokenize",
            "features",
            "input",
            "gs://bucket/features.npy",
            "stage-preprocess",
            sv_preprocess,
        )
        test_db.add_signal("stage-tokenize", "tokens", "npy", "gs://bucket/tokens.npy")

        # Level 3: Train (depends on tokenize)
        sv_train, _, _ = test_db.get_or_create_stage_version("test-ws", "train", "sha1", "c" * 64)
        test_db.create_stage_run(
            stage_run_id="stage-train",
            workspace_name="test-ws",
            version="v1",
            stage_name="train",
        )
        test_db.update_stage_run_version("stage-train", sv_train)
        _transition_to_completed(test_db, "stage-train")
        test_db.add_signal_with_source(
            "stage-train", "tokens", "input", "gs://bucket/tokens.npy", "stage-tokenize", sv_tokenize
        )

        # Query train's lineage - should show full chain
        lineage = test_db.get_lineage_tree("stage-train")

        assert lineage["stage"] == "train"
        assert "tokens" in lineage["inputs"]

        # Check tokenize level
        tokenize_input = lineage["inputs"]["tokens"]
        assert tokenize_input["source_stage"] == "tokenize"
        assert "upstream" in tokenize_input

        # Check preprocess level (nested in tokenize's upstream)
        tokenize_upstream = tokenize_input["upstream"]
        assert tokenize_upstream["stage"] == "tokenize"
        assert "features" in tokenize_upstream["inputs"]

        preprocess_input = tokenize_upstream["inputs"]["features"]
        assert preprocess_input["source_stage"] == "preprocess"

    def test_get_lineage_tree_max_depth(self, test_db):
        """Lineage should respect max_depth limit."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Create a 5-level chain: stage1 -> stage2 -> stage3 -> stage4 -> stage5
        prev_run_id = None
        prev_sv_id = None

        for i in range(1, 6):
            sv_id, _, _ = test_db.get_or_create_stage_version("test-ws", f"stage{i}", "sha1", f"{chr(96 + i)}" * 64)
            run_id = f"stage-{i:03d}"

            test_db.create_stage_run(
                stage_run_id=run_id,
                workspace_name="test-ws",
                version="v1",
                stage_name=f"stage{i}",
            )
            test_db.update_stage_run_version(run_id, sv_id)
            _transition_to_completed(test_db, run_id)

            if prev_run_id:
                test_db.add_signal_with_source(
                    run_id, "data", "input", f"gs://bucket/data{i}.npy", prev_run_id, prev_sv_id
                )

            test_db.add_signal(run_id, "output", "npy", f"gs://bucket/output{i}.npy")

            prev_run_id = run_id
            prev_sv_id = sv_id

        # Query with max_depth=2 - should only show 2 levels
        lineage = test_db.get_lineage_tree("stage-005", max_depth=2)

        # Verify depth is limited
        assert lineage["stage"] == "stage5"
        if lineage["inputs"]:
            level1 = list(lineage["inputs"].values())[0]
            if "upstream" in level1:
                level2 = level1["upstream"]
                # Should not go deeper than max_depth
                if level2["inputs"]:
                    level2_input = list(level2["inputs"].values())[0]
                    assert "upstream" not in level2_input or level2_input.get("upstream") is None


class TestDownstreamRunQueries:
    """Tests for finding runs that used a specific stage version."""

    def test_get_downstream_runs_empty(self, test_db):
        """Stage version with no downstream consumers."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        sv_id, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)

        runs = test_db.get_downstream_runs(sv_id)
        assert runs == []

    def test_get_downstream_runs_single(self, test_db):
        """Stage version used by one downstream run."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Create preprocess
        sv_preprocess, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)
        test_db.create_stage_run("stage-pre", "test-ws", "v1", "preprocess")
        test_db.update_stage_run_version("stage-pre", sv_preprocess)
        _transition_to_completed(test_db, "stage-pre")
        test_db.add_signal("stage-pre", "features", "npy", "gs://features.npy")

        # Create train that uses preprocess
        sv_train, _, _ = test_db.get_or_create_stage_version("test-ws", "train", "sha1", "b" * 64)
        test_db.create_stage_run("stage-train", "test-ws", "v1", "train")
        test_db.update_stage_run_version("stage-train", sv_train)
        test_db.add_signal_with_source(
            "stage-train", "features", "input", "gs://features.npy", "stage-pre", sv_preprocess
        )

        # Query downstream runs of preprocess
        runs = test_db.get_downstream_runs(sv_preprocess)
        assert len(runs) == 1
        assert runs[0]["id"] == "stage-train"

    def test_get_downstream_runs_multiple(self, test_db):
        """Stage version used by multiple downstream runs."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Create preprocess
        sv_preprocess, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)
        test_db.create_stage_run("stage-pre", "test-ws", "v1", "preprocess")
        test_db.update_stage_run_version("stage-pre", sv_preprocess)
        _transition_to_completed(test_db, "stage-pre")
        test_db.add_signal("stage-pre", "features", "npy", "gs://features.npy")

        # Create 3 downstream stages that all use preprocess
        for i in range(3):
            sv_id, _, _ = test_db.get_or_create_stage_version("test-ws", f"train{i}", "sha1", f"{chr(98 + i)}" * 64)
            test_db.create_stage_run(f"stage-train{i}", "test-ws", "v1", f"train{i}")
            test_db.update_stage_run_version(f"stage-train{i}", sv_id)
            test_db.add_signal_with_source(
                f"stage-train{i}", "features", "input", "gs://features.npy", "stage-pre", sv_preprocess
            )

        runs = test_db.get_downstream_runs(sv_preprocess)
        assert len(runs) == 3


class TestLatestCompletedRunQuery:
    """Tests for getting most recent completed run."""

    def test_get_latest_completed_stage_run(self, test_db):
        """Should return the most recent completed run."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Create 3 runs in sequence
        for i in range(3):
            sv_id, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", f"sha{i}", "a" * 64)
            test_db.create_stage_run(f"stage-{i:03d}", "test-ws", "v1", "preprocess")
            test_db.update_stage_run_version(f"stage-{i:03d}", sv_id)
            _transition_to_completed(test_db, f"stage-{i:03d}")

        latest = test_db.get_latest_completed_stage_run("test-ws", "preprocess")
        assert latest is not None
        assert latest["id"] == "stage-002"  # Most recent

    def test_get_latest_completed_ignores_failed(self, test_db):
        """Should skip failed runs."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # First run - completed
        sv1, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)
        test_db.create_stage_run("stage-001", "test-ws", "v1", "preprocess")
        test_db.update_stage_run_version("stage-001", sv1)
        _transition_to_completed(test_db, "stage-001")

        # Second run - failed (should be skipped)
        sv2, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha2", "b" * 64)
        test_db.create_stage_run("stage-002", "test-ws", "v1", "preprocess")
        test_db.update_stage_run_version("stage-002", sv2)
        _transition_to_failed(test_db, "stage-002")

        latest = test_db.get_latest_completed_stage_run("test-ws", "preprocess")
        assert latest is not None
        assert latest["id"] == "stage-001"  # Skipped failed run

    def test_get_latest_completed_none_available(self, test_db):
        """Should return None when no completed runs exist."""
        test_db.create_workspace_lineage("test-ws", description="Test")
        test_db.create_version("test-ws", "v1", "test-ws-v1", "sha1", "run")

        # Only running/pending runs
        sv_id, _, _ = test_db.get_or_create_stage_version("test-ws", "preprocess", "sha1", "a" * 64)
        test_db.create_stage_run("stage-001", "test-ws", "v1", "preprocess")
        test_db.update_stage_run_version("stage-001", sv_id)
        # No status update - stays pending

        latest = test_db.get_latest_completed_stage_run("test-ws", "preprocess")
        assert latest is None
