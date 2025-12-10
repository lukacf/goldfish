"""Deluxe end-to-end test with real GCE execution.

This test simulates a complete ML research workflow:
1. Initialize Goldfish project with ML pipeline
2. Run baseline pipeline (all stages)
3. Iterate with improved hyperparameters
4. Compare results using lineage tracking

IMPORTANT: This test launches real GCE instances and incurs cloud costs.
Run only when GOLDFISH_DELUXE_TEST_ENABLED=1 is set.
"""

import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from goldfish.config import GoldfishConfig
from tests.e2e.deluxe.conftest import skip_if_not_enabled, is_dry_run


@pytest.mark.deluxe_gce
@pytest.mark.timeout(1800)  # 30 minute timeout
class TestDeluxeGCEEndToEnd:
    """Comprehensive E2E test with real GCE execution."""

    def test_full_ml_workflow(
        self,
        deluxe_project,
        ml_project_template,
        gce_cleanup,
    ):
        """Complete ML workflow: baseline → iterate → compare.

        Workflow:
        1. Create workspace "baseline"
        2. Copy ML pipeline template
        3. Run full pipeline (generate_data → preprocess → train → evaluate)
        4. Verify baseline results
        5. Create workspace "improved" from baseline
        6. Update training config (increase max_iter)
        7. Re-run train → evaluate stages
        8. Compare baseline vs improved metrics
        9. Verify lineage tracking
        """
        skip_if_not_enabled()

        workspace_manager = deluxe_project["workspace_manager"]
        pipeline_manager = deluxe_project["pipeline_manager"]
        stage_executor = deluxe_project["stage_executor"]
        db = deluxe_project["db"]
        project_root = deluxe_project["project_root"]
        gce_config = deluxe_project["gce_config"]

        # ======================================================================
        # PHASE 1: Project Setup
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 1: Project Setup")
        print("=" * 70)

        # Create workspace "baseline"
        workspace_manager.create_workspace(
            name="baseline",
            goal="Baseline ML classification model",
            reason="Deluxe E2E test - baseline run",
        )
        print("✓ Created workspace 'baseline'")

        # Mount to w1
        workspace_manager.mount(
            workspace="baseline",
            slot="w1",
            reason="Deluxe E2E test setup",
        )
        print("✓ Mounted workspace to w1")

        workspace_path = project_root / "workspaces" / "w1"

        # Copy ML project template
        for item in ["modules", "configs", "pipeline.yaml", "requirements.txt"]:
            src = ml_project_template / item
            dst = workspace_path / item
            if src.is_dir():
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)
        print("✓ Copied ML project template")

        # Validate pipeline
        errors = pipeline_manager.validate_pipeline("baseline")
        assert len(errors) == 0, f"Pipeline validation failed: {errors}"
        print("✓ Pipeline validated successfully")

        # Checkpoint
        workspace_manager.checkpoint(
            slot="w1",
            message="Initial ML pipeline setup",
        )
        print("✓ Checkpointed workspace")

        # ======================================================================
        # PHASE 2: Baseline Pipeline Run
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 2: Baseline Pipeline Run")
        print("=" * 70)

        baseline_runs = {}

        # Stage 1: generate_data (local)
        print("\n--- Running: generate_data ---")
        run1 = stage_executor.run_stage(
            workspace="baseline",
            stage_name="generate_data",
            reason="Deluxe E2E test - generate synthetic data",
        )
        baseline_runs["generate_data"] = run1.stage_run_id
        print(f"✓ Launched generate_data: {run1.stage_run_id}")

        # Wait for completion
        if not is_dry_run():
            final_status = stage_executor.wait_for_completion(
                run1.stage_run_id, poll_interval=5, timeout=300
            )
            assert final_status == "completed", f"generate_data failed: {final_status}"
            print(f"✓ generate_data completed successfully")

        # Stage 2: preprocess (GCE with cpu-small)
        print("\n--- Running: preprocess ---")
        run2 = stage_executor.run_stage(
            workspace="baseline",
            stage_name="preprocess",
            reason="Deluxe E2E test - preprocess data",
        )
        baseline_runs["preprocess"] = run2.stage_run_id
        print(f"✓ Launched preprocess: {run2.stage_run_id}")

        # Register cleanup for GCE instance
        if not is_dry_run():
            gce_cleanup.append(
                lambda: self._cleanup_instance(run2.stage_run_id, gce_config["zone"])
            )

            final_status = stage_executor.wait_for_completion(
                run2.stage_run_id, poll_interval=10, timeout=600
            )
            assert final_status == "completed", f"preprocess failed: {final_status}"
            print(f"✓ preprocess completed successfully")

        # Stage 3: train (GCE with cpu-small)
        print("\n--- Running: train ---")
        run3 = stage_executor.run_stage(
            workspace="baseline",
            stage_name="train",
            reason="Deluxe E2E test - train baseline model",
        )
        baseline_runs["train"] = run3.stage_run_id
        print(f"✓ Launched train: {run3.stage_run_id}")

        if not is_dry_run():
            gce_cleanup.append(
                lambda: self._cleanup_instance(run3.stage_run_id, gce_config["zone"])
            )

            final_status = stage_executor.wait_for_completion(
                run3.stage_run_id, poll_interval=10, timeout=900
            )
            assert final_status == "completed", f"train failed: {final_status}"
            print(f"✓ train completed successfully")

        # Stage 4: evaluate (GCE with cpu-small)
        print("\n--- Running: evaluate ---")
        run4 = stage_executor.run_stage(
            workspace="baseline",
            stage_name="evaluate",
            reason="Deluxe E2E test - evaluate baseline model",
        )
        baseline_runs["evaluate"] = run4.stage_run_id
        print(f"✓ Launched evaluate: {run4.stage_run_id}")

        if not is_dry_run():
            gce_cleanup.append(
                lambda: self._cleanup_instance(run4.stage_run_id, gce_config["zone"])
            )

            final_status = stage_executor.wait_for_completion(
                run4.stage_run_id, poll_interval=10, timeout=600
            )
            assert final_status == "completed", f"evaluate failed: {final_status}"
            print(f"✓ evaluate completed successfully")

        # ======================================================================
        # PHASE 3: Verify Baseline Results
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 3: Verify Baseline Results")
        print("=" * 70)

        if not is_dry_run():
            # Retrieve baseline metrics
            baseline_metrics = self._get_metrics(
                project_root, baseline_runs["evaluate"]
            )
            print(f"✓ Baseline test accuracy: {baseline_metrics['test_accuracy']:.4f}")
            assert baseline_metrics["test_accuracy"] > 0.5, "Baseline accuracy too low"

        # Verify stage runs in database
        with db._conn() as conn:
            stage_runs = conn.execute(
                "SELECT * FROM stage_runs WHERE workspace_name = ?",
                ("baseline",),
            ).fetchall()

        assert len(stage_runs) >= 4, f"Expected 4+ stage runs, got {len(stage_runs)}"
        print(f"✓ Found {len(stage_runs)} stage runs in database")

        # Verify signal lineage
        with db._conn() as conn:
            signals = conn.execute("SELECT * FROM signal_lineage").fetchall()

        print(f"✓ Found {len(signals)} signals in lineage")

        # ======================================================================
        # PHASE 4: Iteration - Improved Model
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 4: Iteration - Improved Model")
        print("=" * 70)

        # Create new workspace branched from baseline
        # Get current baseline version
        with db._conn() as conn:
            versions = conn.execute(
                "SELECT version FROM workspace_versions WHERE workspace_name = ? ORDER BY created_at DESC LIMIT 1",
                ("baseline",),
            ).fetchone()

        baseline_version = versions["version"]
        print(f"✓ Baseline version: {baseline_version}")

        # Branch workspace
        from goldfish.lineage.manager import LineageManager

        lineage_manager = LineageManager(db, workspace_manager)
        lineage_manager.branch_workspace(
            from_workspace="baseline",
            from_version=baseline_version,
            new_workspace="improved",
            reason="Deluxe E2E test - improved hyperparameters",
        )
        print("✓ Created workspace 'improved' from baseline")

        # Mount improved workspace to w2
        workspace_manager.mount(
            workspace="improved",
            slot="w2",
            reason="Deluxe E2E test - iteration",
        )
        print("✓ Mounted workspace 'improved' to w2")

        workspace_path_improved = project_root / "workspaces" / "w2"

        # Update train config (increase max_iter)
        train_config_path = workspace_path_improved / "configs" / "train.yaml"
        train_config = train_config_path.read_text()
        train_config = train_config.replace('MAX_ITER: "100"', 'MAX_ITER: "500"')
        train_config_path.write_text(train_config)
        print("✓ Updated train config (MAX_ITER: 100 → 500)")

        # Checkpoint improved workspace
        workspace_manager.checkpoint(
            slot="w2",
            message="Increased max_iter to 500",
        )
        print("✓ Checkpointed improved workspace")

        # Re-run train and evaluate stages
        improved_runs = {}

        print("\n--- Running: train (improved) ---")
        run5 = stage_executor.run_stage(
            workspace="improved",
            stage_name="train",
            config_override=None,  # Config already updated in file
            inputs_override={
                "processed": baseline_runs["preprocess"],  # Reuse preprocessing
            },
            reason="Deluxe E2E test - train improved model",
        )
        improved_runs["train"] = run5.stage_run_id
        print(f"✓ Launched train (improved): {run5.stage_run_id}")

        if not is_dry_run():
            gce_cleanup.append(
                lambda: self._cleanup_instance(run5.stage_run_id, gce_config["zone"])
            )

            final_status = stage_executor.wait_for_completion(
                run5.stage_run_id, poll_interval=10, timeout=900
            )
            assert final_status == "completed", f"train (improved) failed: {final_status}"
            print(f"✓ train (improved) completed successfully")

        print("\n--- Running: evaluate (improved) ---")
        run6 = stage_executor.run_stage(
            workspace="improved",
            stage_name="evaluate",
            inputs_override={
                "processed": baseline_runs["preprocess"],  # Reuse preprocessing
                "model": improved_runs["train"],  # Use improved model
            },
            reason="Deluxe E2E test - evaluate improved model",
        )
        improved_runs["evaluate"] = run6.stage_run_id
        print(f"✓ Launched evaluate (improved): {run6.stage_run_id}")

        if not is_dry_run():
            gce_cleanup.append(
                lambda: self._cleanup_instance(run6.stage_run_id, gce_config["zone"])
            )

            final_status = stage_executor.wait_for_completion(
                run6.stage_run_id, poll_interval=10, timeout=600
            )
            assert final_status == "completed", f"evaluate (improved) failed: {final_status}"
            print(f"✓ evaluate (improved) completed successfully")

        # ======================================================================
        # PHASE 5: Compare Results
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 5: Compare Results")
        print("=" * 70)

        if not is_dry_run():
            # Retrieve improved metrics
            improved_metrics = self._get_metrics(
                project_root, improved_runs["evaluate"]
            )
            print(
                f"✓ Improved test accuracy: {improved_metrics['test_accuracy']:.4f}"
            )

            # Compare
            print("\nComparison:")
            print(f"  Baseline:  {baseline_metrics['test_accuracy']:.4f}")
            print(f"  Improved:  {improved_metrics['test_accuracy']:.4f}")
            print(
                f"  Delta:     {improved_metrics['test_accuracy'] - baseline_metrics['test_accuracy']:+.4f}"
            )

            # Verify improvement (may not always improve, but should be close)
            assert (
                abs(improved_metrics["test_accuracy"] - baseline_metrics["test_accuracy"]) < 0.3
            ), "Metrics diverged too much"

        # Verify lineage tracking
        lineage = lineage_manager.get_workspace_lineage("improved")
        assert lineage["parent"] == "baseline", "Lineage not tracked correctly"
        print("✓ Lineage tracking verified")

        # ======================================================================
        # PHASE 6: Cleanup & Summary
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 6: Test Complete")
        print("=" * 70)

        print("\nSummary:")
        print(f"  Baseline runs:  {len(baseline_runs)} stages")
        print(f"  Improved runs:  {len(improved_runs)} stages")
        print(f"  Total stage runs: {len(baseline_runs) + len(improved_runs)}")

        if not is_dry_run():
            print(f"  Baseline accuracy: {baseline_metrics['test_accuracy']:.4f}")
            print(f"  Improved accuracy: {improved_metrics['test_accuracy']:.4f}")

        print("\n✓ Deluxe E2E test completed successfully!")

    def _cleanup_instance(self, instance_name: str, zone: str):
        """Delete GCE instance."""
        # Sanitize instance name (same as GCELauncher)
        instance_name = instance_name.replace("_", "-").lower()[:60]

        try:
            subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "delete",
                    instance_name,
                    f"--zone={zone}",
                    "--quiet",
                ],
                capture_output=True,
                check=False,  # Don't fail if already deleted
            )
            print(f"✓ Cleaned up instance: {instance_name}")
        except Exception as e:
            print(f"⚠ Failed to cleanup instance {instance_name}: {e}")

    def _get_metrics(self, project_root: Path, stage_run_id: str) -> dict:
        """Retrieve metrics from stage run output."""
        run_dir = project_root / ".goldfish" / "runs" / stage_run_id
        metrics_file = run_dir / "outputs" / "metrics" / "metrics.json"

        if not metrics_file.exists():
            raise FileNotFoundError(f"Metrics not found: {metrics_file}")

        with open(metrics_file) as f:
            return json.load(f)
