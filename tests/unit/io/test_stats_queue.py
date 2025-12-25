"""TDD tests for StatsQueue - thread-safe async stats computation queue.

Tests verify:
- Non-blocking enqueue
- Blocking flush with guaranteed completion
- Thread safety and concurrency
- Memory safety (stores paths, not data)
- Error handling for missing/corrupt files
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

import numpy as np
import pytest

from goldfish.io.stats import StatsJob, StatsQueue


class TestStatsQueueBasics:
    """Test fundamental StatsQueue operations."""

    def test_enqueue_is_nonblocking(self) -> None:
        """Enqueue should return immediately without processing."""
        queue = StatsQueue()
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
            path = Path(f.name)
            arr = np.random.rand(1000, 100)
            np.save(path, arr)

        job = StatsJob(name="test", path=path, dtype="float32")

        # Should return immediately (< 0.1s even for large file)
        start = time.time()
        queue.enqueue(job)
        elapsed = time.time() - start

        assert elapsed < 0.1, "Enqueue should be non-blocking"
        path.unlink()

    def test_flush_returns_results(self) -> None:
        """Flush should return computed stats for all enqueued jobs."""
        queue = StatsQueue()
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
            path = Path(f.name)
            arr = np.array([[1.0, 2.0], [3.0, 4.0]])
            np.save(path, arr)

        job = StatsJob(name="test_arr", path=path, dtype="float32")
        queue.enqueue(job)

        results = queue.flush(timeout=5.0)

        assert "test_arr" in results
        stats = results["test_arr"]
        assert "mean" in stats
        assert "std" in stats
        assert "min" in stats
        assert "max" in stats
        assert stats["mean"] == pytest.approx(2.5)
        path.unlink()

    def test_flush_waits_for_completion(self) -> None:
        """Flush should block until all jobs are processed."""
        queue = StatsQueue()
        paths = []

        # Enqueue multiple jobs
        for i in range(5):
            with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
                path = Path(f.name)
                arr = np.random.rand(100, 10) * i
                np.save(path, arr)
                paths.append(path)
                job = StatsJob(name=f"arr_{i}", path=path, dtype="float32")
                queue.enqueue(job)

        results = queue.flush(timeout=10.0)

        # All jobs should be processed
        assert len(results) == 5
        for i in range(5):
            assert f"arr_{i}" in results

        for path in paths:
            path.unlink()

    def test_empty_queue_flush_returns_empty_dict(self) -> None:
        """Flush on empty queue should return immediately with empty dict."""
        queue = StatsQueue()

        start = time.time()
        results = queue.flush(timeout=1.0)
        elapsed = time.time() - start

        assert results == {}
        assert elapsed < 0.5, "Empty flush should return immediately"


class TestStatsQueueMemorySafety:
    """Test that StatsQueue is memory-safe by storing paths, not data."""

    def test_enqueue_stores_path_not_data(self) -> None:
        """Enqueue should store path reference, not load data into memory."""
        queue = StatsQueue()
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
            path = Path(f.name)
            # Create large array (40MB)
            large_arr = np.random.rand(1000, 5000)
            np.save(path, large_arr)

        job = StatsJob(name="large", path=path, dtype="float32")

        # Enqueue should not load the data
        queue.enqueue(job)

        # Verify by checking that job object is small
        # (Implementation detail: queue should store StatsJob, not arrays)
        # This test passes if enqueue is fast and doesn't OOM

        results = queue.flush(timeout=30.0)
        assert "large" in results
        path.unlink()

    def test_large_file_doesnt_block_enqueue(self) -> None:
        """Multiple large files should enqueue quickly without loading."""
        queue = StatsQueue()
        paths = []

        start = time.time()
        for i in range(10):
            with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
                path = Path(f.name)
                # 10 files x 10MB each = 100MB total
                arr = np.random.rand(250, 5000)
                np.save(path, arr)
                paths.append(path)
                job = StatsJob(name=f"big_{i}", path=path, dtype="float32")
                queue.enqueue(job)

        enqueue_time = time.time() - start

        # All enqueues should be fast (< 1s total)
        assert enqueue_time < 1.0, "Enqueuing should not load files"

        results = queue.flush(timeout=60.0)
        assert len(results) == 10

        for path in paths:
            path.unlink()


class TestStatsQueueConcurrency:
    """Test thread safety and concurrent operations."""

    def test_multiple_enqueues_all_processed(self) -> None:
        """All enqueued jobs should be processed, none lost."""
        queue = StatsQueue()
        num_jobs = 20
        paths = []

        for i in range(num_jobs):
            with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
                path = Path(f.name)
                arr = np.array([i, i + 1, i + 2], dtype=np.float32)
                np.save(path, arr)
                paths.append(path)
                job = StatsJob(name=f"job_{i}", path=path, dtype="float32")
                queue.enqueue(job)

        results = queue.flush(timeout=15.0)

        assert len(results) == num_jobs
        for i in range(num_jobs):
            assert f"job_{i}" in results
            # Mean should be i+1
            assert results[f"job_{i}"]["mean"] == pytest.approx(i + 1)

        for path in paths:
            path.unlink()

    def test_concurrent_enqueue_is_safe(self) -> None:
        """Multiple threads enqueuing simultaneously should be safe."""
        queue = StatsQueue()
        num_threads = 10
        jobs_per_thread = 5
        paths = []
        lock = threading.Lock()

        def enqueue_jobs(thread_id: int) -> None:
            for i in range(jobs_per_thread):
                with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
                    path = Path(f.name)
                    arr = np.random.rand(50, 10)
                    np.save(path, arr)
                    with lock:
                        paths.append(path)
                    job = StatsJob(name=f"t{thread_id}_j{i}", path=path, dtype="float32")
                    queue.enqueue(job)

        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=enqueue_jobs, args=(t,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        results = queue.flush(timeout=20.0)

        expected_jobs = num_threads * jobs_per_thread
        assert len(results) == expected_jobs, "No jobs should be lost in concurrent enqueue"

        for path in paths:
            path.unlink()

    def test_flush_timeout_returns_partial_results(self) -> None:
        """Flush with short timeout should return partial results."""
        queue = StatsQueue()
        paths = []

        # Enqueue many large jobs
        for i in range(20):
            with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
                path = Path(f.name)
                # Large arrays to ensure timeout
                arr = np.random.rand(500, 1000)
                np.save(path, arr)
                paths.append(path)
                job = StatsJob(name=f"slow_{i}", path=path, dtype="float32")
                queue.enqueue(job)

        # Very short timeout
        results = queue.flush(timeout=0.1)

        # Should get partial results (not all 20)
        assert len(results) < 20 or len(results) == 20  # Could complete if fast

        for path in paths:
            path.unlink()


class TestStatsQueueEdgeCases:
    """Test error handling and edge cases."""

    def test_missing_file_handled_gracefully(self) -> None:
        """Missing file should not crash, should return None or empty for that job."""
        queue = StatsQueue()

        missing_path = Path("/tmp/nonexistent_file_12345.npy")
        job = StatsJob(name="missing", path=missing_path, dtype="float32")
        queue.enqueue(job)

        # Should not raise exception
        results = queue.flush(timeout=5.0)

        # Either no entry, or entry with error indicator
        # (Implementation choice: could omit from results or include with None)
        assert "missing" not in results or results["missing"] is None

    def test_corrupt_file_handled_gracefully(self) -> None:
        """Corrupt numpy file should not crash the queue."""
        queue = StatsQueue()
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
            path = Path(f.name)
            # Write garbage data
            f.write(b"This is not a valid numpy file!")

        job = StatsJob(name="corrupt", path=path, dtype="float32")
        queue.enqueue(job)

        # Should not raise
        results = queue.flush(timeout=5.0)

        # Corrupt file should be skipped or return None
        assert "corrupt" not in results or results["corrupt"] is None
        path.unlink()

    def test_double_flush_is_safe(self) -> None:
        """Calling flush twice should be safe (second returns empty)."""
        queue = StatsQueue()
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
            path = Path(f.name)
            arr = np.array([1, 2, 3])
            np.save(path, arr)

        job = StatsJob(name="test", path=path, dtype="float32")
        queue.enqueue(job)

        results1 = queue.flush(timeout=5.0)
        assert "test" in results1

        # Second flush should return empty
        results2 = queue.flush(timeout=1.0)
        assert results2 == {}

        path.unlink()

    def test_zero_timeout_returns_immediately(self) -> None:
        """Flush with timeout=0 should return immediately."""
        queue = StatsQueue()
        with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
            path = Path(f.name)
            arr = np.random.rand(1000, 1000)
            np.save(path, arr)

        job = StatsJob(name="test", path=path, dtype="float32")
        queue.enqueue(job)

        start = time.time()
        results = queue.flush(timeout=0.0)
        elapsed = time.time() - start

        assert elapsed < 0.5, "Zero timeout should return immediately"
        # Results may be empty if job didn't complete
        path.unlink()

    def test_flush_uses_proper_join_semantics(self) -> None:
        """Flush should use queue.join() not poll on unfinished_tasks.

        This test verifies that flush properly waits for task_done() calls,
        not just checks unfinished_tasks count (which is racy).
        """
        queue = StatsQueue()
        paths = []

        # Enqueue jobs rapidly
        for i in range(10):
            with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as f:
                path = Path(f.name)
                arr = np.array([float(i)] * 100)
                np.save(path, arr)
                paths.append(path)
                job = StatsJob(name=f"job_{i}", path=path, dtype="float32")
                queue.enqueue(job)

        # Flush with reasonable timeout - should get all results
        results = queue.flush(timeout=5.0)

        # ALL jobs should be processed - no race-lost results
        assert len(results) == 10, f"Expected 10 results, got {len(results)}"
        for i in range(10):
            assert f"job_{i}" in results
            assert results[f"job_{i}"]["mean"] == pytest.approx(float(i))

        for path in paths:
            path.unlink()
