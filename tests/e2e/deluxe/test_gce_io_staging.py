"""Deluxe E2E test for GCE container I/O staging.

This test validates that:
1. goldfish.io module is correctly packaged in Docker images
2. Input staging from GCS to /mnt/inputs works (via gcsfuse symlinks or gsutil)
3. Output staging from /mnt/outputs to GCS works
4. goldfish.io load_input/save_output functions work correctly

IMPORTANT: This test launches real GCE instances and incurs cloud costs.
Run only when GOLDFISH_DELUXE_TEST_ENABLED=1 is set.
"""

import shutil
import subprocess

import pytest

from .conftest import is_dry_run, skip_if_not_enabled


@pytest.mark.deluxe_gce
@pytest.mark.timeout(900)  # 15 minute timeout
class TestGCEIOStaging:
    """Test goldfish.io I/O staging in GCE containers."""

    def test_goldfish_io_in_gce_container(
        self,
        deluxe_project,
        io_test_template,
        gce_cleanup,
    ):
        """Test goldfish.io works correctly in GCE containers.

        Workflow:
        1. Create workspace with I/O test pipeline
        2. Run generate_test_data stage (local)
        3. Run validate_io stage (GCE) - validates goldfish.io import & functions
        4. Verify outputs were correctly staged back to GCS

        This specifically tests the fix for:
        - goldfish.io module packaging in Docker images
        - Input staging via gcsfuse/gsutil
        - Output staging via gsutil
        """
        skip_if_not_enabled()

        workspace_manager = deluxe_project["workspace_manager"]
        pipeline_manager = deluxe_project["pipeline_manager"]
        stage_executor = deluxe_project["stage_executor"]
        db = deluxe_project["db"]
        project_root = deluxe_project["project_root"]
        gce_config = deluxe_project["gce_config"]

        # ======================================================================
        # PHASE 1: Setup
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 1: Project Setup")
        print("=" * 70)

        # Create workspace
        workspace_manager.create_workspace(
            name="io-test",
            goal="Validate goldfish.io in GCE containers",
            reason="Deluxe E2E test - I/O staging validation",
        )
        print("Created workspace 'io-test'")

        # Mount to w1
        workspace_manager.mount(
            workspace="io-test",
            slot="w1",
            reason="Deluxe E2E I/O test setup",
        )
        print("Mounted workspace to w1")

        workspace_path = project_root / "workspaces" / "w1"

        # Copy I/O test template
        for item in ["modules", "configs", "pipeline.yaml", "requirements.txt"]:
            src = io_test_template / item
            dst = workspace_path / item
            if src.is_dir():
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)
        print("Copied I/O test template")

        # Validate pipeline
        errors = pipeline_manager.validate_pipeline("io-test")
        assert len(errors) == 0, f"Pipeline validation failed: {errors}"
        print("Pipeline validated successfully")

        # Checkpoint
        workspace_manager.checkpoint(
            slot="w1",
            message="Initial I/O test pipeline setup",
        )
        print("Checkpointed workspace")

        # ======================================================================
        # PHASE 2: Generate Test Data (Local)
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 2: Generate Test Data (Local)")
        print("=" * 70)

        run1 = stage_executor.run_stage(
            workspace="io-test",
            stage_name="generate_test_data",
            reason="Deluxe E2E test - generate test data",
        )
        print(f"Launched generate_test_data: {run1.stage_run_id}")

        if not is_dry_run():
            final_status = stage_executor.wait_for_completion(run1.stage_run_id, poll_interval=5, timeout=300)
            assert final_status == "completed", f"generate_test_data failed: {final_status}"

            # Verify outputs were registered in signal_lineage
            with db._conn() as conn:
                outputs = conn.execute(
                    "SELECT signal_name FROM signal_lineage WHERE stage_run_id = ?",
                    (run1.stage_run_id,),
                ).fetchall()
            output_names = {row["signal_name"] for row in outputs}
            assert "test_array" in output_names, "Missing test_array output"
            assert "test_csv" in output_names, "Missing test_csv output"
            assert "test_directory" in output_names, "Missing test_directory output"
            print("generate_test_data completed - outputs verified")

        # ======================================================================
        # PHASE 3: Validate I/O (GCE) - The Key Test
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 3: Validate goldfish.io (GCE)")
        print("=" * 70)
        print("\nThis stage tests:")
        print("  1. goldfish.io module import in container")
        print("  2. load_input() for npy, csv, directory types")
        print("  3. get_config() function")
        print("  4. save_output() for npy, csv types")
        print()

        run2 = stage_executor.run_stage(
            workspace="io-test",
            stage_name="validate_io",
            reason="Deluxe E2E test - validate goldfish.io in GCE",
        )
        print(f"Launched validate_io: {run2.stage_run_id}")

        if not is_dry_run():
            # Register cleanup for GCE instance
            gce_cleanup.append(lambda: self._cleanup_instance(run2.stage_run_id, gce_config["zone"]))

            final_status = stage_executor.wait_for_completion(run2.stage_run_id, poll_interval=10, timeout=600)

            # Get run details for debugging
            with db._conn() as conn:
                run_row = conn.execute("SELECT * FROM stage_runs WHERE id = ?", (run2.stage_run_id,)).fetchone()
            print("\n--- Stage Run Details ---")
            if run_row:
                print(f"  Status: {run_row['status']}")
                print(f"  Backend: {run_row['backend_type']}")
                print(f"  Error: {run_row.get('error_message', 'None')}")
            print("--- End Details ---\n")

            assert final_status == "completed", f"validate_io failed: {final_status}"
            print("validate_io completed successfully!")

        # ======================================================================
        # PHASE 4: Verify Results
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 4: Verify Results")
        print("=" * 70)

        if not is_dry_run():
            # Verify outputs were staged back from GCE
            with db._conn() as conn:
                outputs = conn.execute(
                    "SELECT signal_name, storage_location FROM signal_lineage WHERE stage_run_id = ?",
                    (run2.stage_run_id,),
                ).fetchall()
            output_dict = {row["signal_name"]: row["storage_location"] for row in outputs}
            assert "validation_results" in output_dict, "Missing validation_results output"
            assert "transformed_array" in output_dict, "Missing transformed_array output"
            print("Output staging verified - all outputs present")

            # Show results location
            print(f"Validation results at: {output_dict.get('validation_results', 'N/A')}")

            # Verify all tests passed
            with db._conn() as conn:
                run_row = conn.execute("SELECT status FROM stage_runs WHERE id = ?", (run2.stage_run_id,)).fetchone()
            assert run_row["status"] == "completed", "Stage run not marked completed"

        # ======================================================================
        # PHASE 5: Summary
        # ======================================================================
        print("\n" + "=" * 70)
        print("PHASE 5: Test Complete")
        print("=" * 70)

        print("\nValidated:")
        print("  goldfish.io module packaging in Docker image")
        print("  Input staging from GCS to /mnt/inputs")
        print("  Output staging from /mnt/outputs to GCS")
        print("  load_input() for npy, csv, directory types")
        print("  save_output() for npy, csv types")
        print("  get_config() function")
        print("\nGCE I/O staging test completed successfully!")

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
            print(f"Cleaned up instance: {instance_name}")
        except Exception as e:
            print(f"Failed to cleanup instance {instance_name}: {e}")
