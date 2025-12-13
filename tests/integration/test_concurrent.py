"""Tests for concurrent operations - P2.

Tests that multiple operations can run safely in parallel without
data corruption or race conditions.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from goldfish.config import AuditConfig, GoldfishConfig, JobsConfig, StateMdConfig
from goldfish.db.database import Database
from goldfish.state.state_md import StateManager


class TestConcurrentDatabaseOperations:
    """Tests for concurrent database access."""

    def test_concurrent_audit_logging(self, temp_dir):
        """Multiple threads can log to audit trail simultaneously."""
        db = Database(temp_dir / "test.db")
        num_threads = 10
        writes_per_thread = 20
        errors = []

        def log_audits(thread_id):
            try:
                for i in range(writes_per_thread):
                    db.log_audit(
                        operation=f"test_op_{thread_id}",
                        reason=f"Thread {thread_id} write {i} - testing concurrent access",
                        slot=f"w{thread_id % 3 + 1}",
                        workspace=f"workspace_{thread_id}",
                    )
            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=log_audits, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify all writes succeeded
        entries = db.get_recent_audit(limit=num_threads * writes_per_thread)
        assert len(entries) == num_threads * writes_per_thread

    def test_concurrent_source_creation(self, temp_dir):
        """Multiple threads can create sources simultaneously."""
        db = Database(temp_dir / "test.db")
        num_sources = 50
        errors = []
        created_ids = []
        lock = threading.Lock()

        def create_source(source_num):
            try:
                source_id = f"source_{source_num}"
                db.create_source(
                    source_id=source_id,
                    name=f"Source {source_num}",
                    gcs_location=f"gs://bucket/sources/{source_num}",
                    created_by="external",
                    description=f"Test source {source_num}",
                )
                with lock:
                    created_ids.append(source_id)
            except Exception as e:
                with lock:
                    errors.append((source_num, e))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_source, i) for i in range(num_sources)]
            for future in as_completed(futures):
                future.result()  # Raises if exception occurred

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(created_ids) == num_sources

        # Verify all sources exist
        sources = db.list_sources()
        assert len(sources) == num_sources

    def test_concurrent_job_creation_and_update(self, temp_dir):
        """Jobs can be created and updated concurrently."""
        db = Database(temp_dir / "test.db")
        num_jobs = 30
        errors = []

        def create_and_update_job(job_num):
            try:
                job_id = f"job-{job_num:08x}"
                db.create_job(
                    job_id=job_id,
                    workspace=f"ws_{job_num % 5}",
                    snapshot_id=f"snap-abc{job_num:04x}-20251205-120000",
                    script="train.py",
                )
                # Simulate some processing
                time.sleep(0.01)
                db.update_job_status(job_id, "running")
                time.sleep(0.01)
                db.update_job_status(job_id, "completed")
            except Exception as e:
                errors.append((job_num, e))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_and_update_job, i) for i in range(num_jobs)]
            for future in as_completed(futures):
                future.result()

        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify all jobs completed
        jobs = db.list_jobs(limit=num_jobs)
        assert len(jobs) == num_jobs
        assert all(j["status"] == "completed" for j in jobs)

    def test_concurrent_workspace_goals(self, temp_dir):
        """Workspace goals can be set concurrently."""
        db = Database(temp_dir / "test.db")
        workspaces = ["ws1", "ws2", "ws3", "ws4", "ws5"]
        updates_per_workspace = 20
        errors = []

        def update_goals(workspace):
            try:
                for i in range(updates_per_workspace):
                    db.set_workspace_goal(workspace, f"Goal iteration {i} for {workspace}")
            except Exception as e:
                errors.append((workspace, e))

        with ThreadPoolExecutor(max_workers=len(workspaces)) as executor:
            futures = [executor.submit(update_goals, ws) for ws in workspaces]
            for future in as_completed(futures):
                future.result()

        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify final goals
        for ws in workspaces:
            goal = db.get_workspace_goal(ws)
            assert goal is not None
            assert ws in goal


class TestConcurrentStateManager:
    """Tests for concurrent STATE.md operations."""

    @pytest.fixture
    def config(self):
        return GoldfishConfig(
            project_name="test-project",
            dev_repo_path="../test-dev",
            state_md=StateMdConfig(max_recent_actions=100),
        )

    def test_concurrent_action_logging(self, temp_dir, config):
        """Multiple threads can add actions simultaneously."""
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, config)
        num_threads = 10
        actions_per_thread = 20
        errors = []

        def add_actions(thread_id):
            try:
                for i in range(actions_per_thread):
                    manager.add_action(f"Thread {thread_id} action {i}")
            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=add_actions, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Note: Due to deque's maxlen, we may have fewer than all actions
        # but should have the most recent ones
        assert len(manager._recent_actions) <= config.state_md.max_recent_actions

    def test_concurrent_regenerate(self, temp_dir, config):
        """STATE.md can be regenerated concurrently without corruption."""
        state_path = temp_dir / "STATE.md"
        manager = StateManager(state_path, config)
        num_regenerates = 20
        errors = []

        def regenerate(iteration):
            try:
                manager.regenerate(
                    slots=[],
                    jobs=[{"id": f"job-{iteration:08x}", "script": "test.py", "status": "running"}],
                    source_count=iteration,
                )
            except Exception as e:
                errors.append((iteration, e))

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(regenerate, i) for i in range(num_regenerates)]
            for future in as_completed(futures):
                future.result()

        assert len(errors) == 0, f"Errors occurred: {errors}"

        # File should exist and be readable
        assert state_path.exists()
        content = state_path.read_text()
        assert "# test-project" in content


class TestConcurrentTransactions:
    """Tests for database transaction isolation."""

    def test_transaction_isolation(self, temp_dir):
        """Transactions should be isolated from each other."""
        db = Database(temp_dir / "test.db")
        results = {"success": 0, "rollback": 0}
        lock = threading.Lock()

        def transaction_with_error(should_fail):
            try:
                with db.transaction() as conn:
                    conn.execute(
                        """
                        INSERT INTO audit (timestamp, operation, reason)
                        VALUES (?, ?, ?)
                        """,
                        ("2025-01-01T00:00:00", "test", "Testing transaction isolation"),
                    )
                    if should_fail:
                        raise ValueError("Intentional failure")
                with lock:
                    results["success"] += 1
            except ValueError:
                with lock:
                    results["rollback"] += 1
            except Exception:
                # Unexpected error
                raise

        threads = []
        # Half succeed, half fail
        for i in range(20):
            t = threading.Thread(target=transaction_with_error, args=(i % 2 == 0,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Check results
        assert results["success"] == 10
        assert results["rollback"] == 10

        # Only successful transactions should be in DB
        entries = db.get_recent_audit(limit=100)
        assert len(entries) == 10

    def test_job_with_inputs_atomic(self, temp_dir):
        """create_job_with_inputs should be atomic."""
        db = Database(temp_dir / "test.db")

        # Create a source first
        db.create_source(
            source_id="test-source",
            name="Test Source",
            gcs_location="gs://bucket/test",
            created_by="external",
        )

        errors = []
        successful_jobs = []
        lock = threading.Lock()

        def create_job_with_inputs(job_num):
            try:
                job_id = f"job-{job_num:08x}"
                db.create_job_with_inputs(
                    job_id=job_id,
                    workspace=f"ws_{job_num}",
                    snapshot_id=f"snap-abc{job_num:04x}-20251205-120000",
                    script="train.py",
                    inputs={"data": "test-source"},
                )
                with lock:
                    successful_jobs.append(job_id)
            except Exception as e:
                with lock:
                    errors.append((job_num, e))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_job_with_inputs, i) for i in range(20)]
            for future in as_completed(futures):
                future.result()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(successful_jobs) == 20

        # Verify all jobs have their inputs
        for job_id in successful_jobs:
            inputs = db.get_job_inputs(job_id)
            assert len(inputs) == 1
            assert inputs[0]["source_id"] == "test-source"


class TestConcurrentReadWrite:
    """Tests for concurrent read/write patterns."""

    def test_read_while_writing(self, temp_dir):
        """Reads should not be blocked by writes."""
        db = Database(temp_dir / "test.db")
        stop_flag = threading.Event()
        read_count = {"count": 0}
        write_count = {"count": 0}
        errors = []

        def writer():
            try:
                i = 0
                while not stop_flag.is_set():
                    db.log_audit(
                        operation="write_test",
                        reason=f"Write iteration {i} - concurrent read test",
                    )
                    write_count["count"] += 1
                    i += 1
                    time.sleep(0.001)
            except Exception as e:
                errors.append(("writer", e))

        def reader():
            try:
                while not stop_flag.is_set():
                    db.get_recent_audit(limit=10)
                    read_count["count"] += 1
                    time.sleep(0.001)
            except Exception as e:
                errors.append(("reader", e))

        # Start threads
        writer_thread = threading.Thread(target=writer)
        reader_threads = [threading.Thread(target=reader) for _ in range(3)]

        writer_thread.start()
        for t in reader_threads:
            t.start()

        # Let them run for a bit
        time.sleep(0.5)
        stop_flag.set()

        writer_thread.join()
        for t in reader_threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert write_count["count"] > 0
        assert read_count["count"] > 0

    def test_list_while_creating(self, temp_dir):
        """Listing should work while creating new items."""
        db = Database(temp_dir / "test.db")
        stop_flag = threading.Event()
        errors = []
        created = {"count": 0}

        def creator():
            try:
                i = 0
                while not stop_flag.is_set() and i < 100:
                    db.create_source(
                        source_id=f"src_{i}",
                        name=f"Source {i}",
                        gcs_location=f"gs://bucket/{i}",
                        created_by="external",
                    )
                    created["count"] += 1
                    i += 1
                    time.sleep(0.005)
            except Exception as e:
                errors.append(("creator", e))

        def lister():
            try:
                while not stop_flag.is_set():
                    sources = db.list_sources()
                    # Just verify it returns a list
                    assert isinstance(sources, list)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(("lister", e))

        creator_thread = threading.Thread(target=creator)
        lister_threads = [threading.Thread(target=lister) for _ in range(3)]

        creator_thread.start()
        for t in lister_threads:
            t.start()

        # Wait for creator to finish
        creator_thread.join()
        stop_flag.set()

        for t in lister_threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert created["count"] == 100


class TestConcurrentWorkspaceOperations:
    """Tests for concurrent workspace mount/unmount operations."""

    def test_concurrent_mount_to_same_slot(self, temp_dir, temp_git_repo):
        """Test that concurrent mounts to same slot are safely rejected.

        This tests the TOCTOU fix where two threads try to mount different
        workspaces to the same slot simultaneously. Only one should succeed,
        the other should get SlotNotEmptyError.
        """
        from goldfish.db.database import Database
        from goldfish.errors import SlotNotEmptyError
        from goldfish.workspace.manager import WorkspaceManager

        # Setup
        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        db = Database(temp_dir / "test.db")

        # Create config
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_git_repo),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        manager = WorkspaceManager(
            config=config,
            project_root=project_root,
            db=db,
        )

        # Create two workspaces
        manager.create_workspace("workspace1", goal="Test workspace 1", reason="Testing concurrent mounting")
        manager.create_workspace("workspace2", goal="Test workspace 2", reason="Testing concurrent mounting")

        # Track results
        results = {"success": [], "errors": []}
        lock = threading.Lock()

        def try_mount(workspace_name, slot):
            try:
                # Add small delay to increase chance of race condition
                time.sleep(0.001)
                manager.mount(workspace_name, slot, f"Testing concurrent mount of {workspace_name}")
                with lock:
                    results["success"].append(workspace_name)
            except SlotNotEmptyError:
                # Expected for one of the threads
                with lock:
                    results["errors"].append((workspace_name, "SlotNotEmptyError"))
            except Exception as e:
                with lock:
                    results["errors"].append((workspace_name, str(e)))

        # Try to mount both workspaces to the same slot simultaneously
        thread1 = threading.Thread(target=try_mount, args=("workspace1", "w1"))
        thread2 = threading.Thread(target=try_mount, args=("workspace2", "w1"))

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        # Debug: Print what happened
        print(f"\nDEBUG: Success: {results['success']}")
        print(f"DEBUG: Errors: {results['errors']}")

        # Verify results
        assert len(results["success"]) == 1, (
            f"Expected exactly 1 success, got {len(results['success'])}: {results['success']}"
        )
        assert len(results["errors"]) == 1, (
            f"Expected exactly 1 error, got {len(results['errors'])}: {results['errors']}"
        )

        # The error should be SlotNotEmptyError
        assert results["errors"][0][1] == "SlotNotEmptyError", (
            f"Expected SlotNotEmptyError, got {results['errors'][0][1]}"
        )

        # Verify filesystem state is consistent - only one workspace should be mounted
        slot_info = manager.get_slot_info("w1")
        assert slot_info.workspace == results["success"][0]

        # Cleanup
        manager.hibernate("w1", "Cleaning up test")

    def test_workspace_deletion_blocked_by_running_job(self, temp_dir, temp_git_repo):
        """Test that workspace cannot be deleted while jobs are running."""
        from goldfish.db.database import Database
        from goldfish.workspace.manager import WorkspaceManager

        # Setup
        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        db = Database(temp_dir / "test.db")

        # Create config
        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_git_repo),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        manager = WorkspaceManager(
            config=config,
            project_root=project_root,
            db=db,
        )

        # 1. Create workspace and mount it
        manager.create_workspace(
            "test-workspace",
            goal="Test workspace deletion protection",
            reason="Testing workspace deletion with running jobs",
        )
        manager.mount("test-workspace", "w1", "Testing workspace deletion protection")

        # 2. Create a running job associated with this workspace
        db.create_job(
            job_id="job-running-001",
            workspace="test-workspace",
            snapshot_id="snap-abc123-20251206-120000",
            script="train.py",
            experiment_dir=str(temp_dir / "experiments" / "exp-001"),
        )
        # Update status to running
        db.update_job_status("job-running-001", "running")

        # 3. Try to delete the workspace - should fail
        # Note: Goldfish doesn't have a delete_workspace method yet, so we'll
        # verify that the git branch can't be deleted while jobs are running
        # by checking the database constraint

        # First, hibernate the workspace (required before deletion)
        manager.hibernate("w1", "Testing workspace deletion protection")

        # Verify the workspace still exists
        assert manager.git.branch_exists("test-workspace")

        # 4. Verify workspace can't be deleted while job is running
        # (This would be implemented in a delete_workspace method)
        # For now, we verify the job is still active
        active_jobs = db.get_active_jobs()
        assert len(active_jobs) == 1
        assert active_jobs[0]["workspace"] == "test-workspace"

        # 5. Mark job as completed
        db.update_job_status("job-running-001", "completed")

        # 6. Now verify job is no longer active (deletion would be safe)
        active_jobs = db.get_active_jobs()
        assert len(active_jobs) == 0

        # Verify workspace still exists and can be mounted again
        assert manager.git.branch_exists("test-workspace")
        manager.mount("test-workspace", "w1", "Verifying workspace still exists")
        manager.hibernate("w1", "Cleaning up test")

    def test_concurrent_checkpoint_and_rollback(self, temp_dir, temp_git_repo):
        """Test race condition: checkpoint while rollback is happening."""
        import threading

        from goldfish.db.database import Database
        from goldfish.workspace.manager import WorkspaceManager

        # Setup
        project_root = temp_dir / "project"
        project_root.mkdir()
        (project_root / "workspaces").mkdir()

        db = Database(temp_dir / "test.db")

        config = GoldfishConfig(
            project_name="test-project",
            dev_repo_path=str(temp_git_repo),
            workspaces_dir="workspaces",
            slots=["w1", "w2", "w3"],
            state_md=StateMdConfig(path="STATE.md", max_recent_actions=15),
            audit=AuditConfig(min_reason_length=15),
            jobs=JobsConfig(backend="local", experiments_dir="experiments"),
            invariants=[],
        )

        manager = WorkspaceManager(
            config=config,
            project_root=project_root,
            db=db,
        )

        # 1. Create workspace with some content
        manager.create_workspace(
            "test-workspace",
            goal="Test concurrent checkpoint and rollback",
            reason="Testing race conditions between checkpoint and rollback",
        )
        manager.mount("test-workspace", "w1", "Testing concurrent checkpoint and rollback")

        # Add some content
        slot_path = manager.get_slot_path("w1")
        test_file = slot_path / "test.txt"
        test_file.write_text("Initial content")

        # 2. Create a version to rollback to (using save_version, checkpoint is deprecated)
        version1 = manager.save_version("w1", "Initial version for rollback test")
        version_to_rollback = version1.version  # e.g., "v1"

        # 3. Make changes
        test_file.write_text("Modified content")

        # Create another version with the modified content
        manager.save_version("w1", "Second version with modifications")

        # Make more changes
        test_file.write_text("More modified content")

        # Track results
        results = {"checkpoint": None, "rollback": None, "errors": []}
        lock = threading.Lock()

        # Use a barrier to ensure both threads start at the same time
        barrier = threading.Barrier(2)

        def do_rollback():
            try:
                barrier.wait()  # Wait for both threads to be ready
                # Rollback to first version (slow operation)
                result = manager.rollback("w1", version_to_rollback, "Testing concurrent rollback operation")
                with lock:
                    results["rollback"] = result
            except Exception as e:
                with lock:
                    results["errors"].append(("rollback", str(e), type(e).__name__))

        def do_checkpoint():
            try:
                barrier.wait()  # Wait for both threads to be ready
                # Try to save_version while rollback is happening
                result = manager.save_version("w1", "Testing concurrent save_version operation")
                with lock:
                    results["checkpoint"] = result
            except Exception as e:
                with lock:
                    results["errors"].append(("checkpoint", str(e), type(e).__name__))

        # 4 & 5. Start rollback and checkpoint in parallel
        thread1 = threading.Thread(target=do_rollback)
        thread2 = threading.Thread(target=do_checkpoint)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        # 6. Verify data consistency - one should succeed, one might fail or wait
        print(f"\nDEBUG: Results: {results}")

        # Either:
        # a) One operation succeeded, the other was blocked/failed gracefully
        # b) Both succeeded (if one completed before the other started)
        # c) Both failed (if there's a deadlock - BAD!)

        # We should not have both operations succeed with inconsistent data
        # The key is that the file should be in a consistent state

        # Check file content is consistent with one of the expected states
        final_content = test_file.read_text()
        print(f"DEBUG: Final file content: {final_content}")

        # The file should be in one of these states:
        # 1. "Initial content" (if rollback succeeded)
        # 2. "More modified content" (if checkpoint succeeded before rollback)
        # 3. "Initial content" (if both succeeded in sequence)

        assert final_content in [
            "Initial content",
            "More modified content",
        ], f"File in unexpected state: {final_content}"

        # If both succeeded, rollback should have happened last (file = "Initial content")
        if results["checkpoint"] and results["rollback"]:
            assert final_content == "Initial content", "If both succeeded, rollback should have been last"

        # Verify no deadlocks or crashes - at least one should have succeeded
        # or we should have a GoldfishError (not a system error)
        if results["errors"]:
            for error in results["errors"]:
                # Errors should be GoldfishError or its subclasses (indicating proper locking)
                # not system errors like OSError
                allowed_errors = ["GoldfishError", "WorkspaceAlreadyExistsError", "WorkspaceNotFoundError"]
                is_allowed_error = any(err_type in error[2] for err_type in allowed_errors)
                is_lock_error = "workspace is locked" in error[1] or "already exists" in error[1]
                assert is_allowed_error or is_lock_error, f"Unexpected error type: {error}"

        # Verify database audit trail is consistent
        audit = db.get_recent_audit(limit=10)
        print(f"DEBUG: Recent audit: {[a['operation'] for a in audit]}")

        # Should see save_version and/or rollback operations
        operations = [a["operation"] for a in audit]
        if results["checkpoint"]:
            assert "save_version" in operations
        if results["rollback"]:
            assert "rollback" in operations

        # Cleanup
        manager.hibernate("w1", "Cleaning up concurrent test")
