"""Thread-safe async stats computation queue.

This module provides a non-blocking queue for computing statistics on numpy arrays.
Jobs are enqueued with file paths (not data) to ensure memory safety, and a background
worker thread processes them asynchronously.

Key features:
- Non-blocking enqueue (< 0.1s)
- Blocking flush with timeout
- Memory-safe (stores paths, not data)
- Thread-safe for concurrent enqueue
- Graceful error handling for missing/corrupt files
"""

from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from goldfish.svs.checks.output_checks import (
    check_entropy,
    check_null_ratio,
    check_vocab_utilization,
)

logger = logging.getLogger(__name__)


@dataclass
class StatsJob:
    """A job to compute statistics on a numpy array file.

    Args:
        name: Identifier for this signal/array
        path: Path to the .npy file (reference, NOT raw data)
        dtype: Expected data type (e.g., "float32")
        sample_size: Maximum number of samples to use for stats (default 10000)
        vocab_size: Optional vocabulary size for token data
    """

    name: str
    path: Path
    dtype: str
    sample_size: int = 10000
    vocab_size: int | None = None


class StatsQueue:
    """Thread-safe queue for async statistics computation.

    Enqueue jobs with file paths, then call flush() to block until all jobs
    are processed and get the results.

    Example:
        queue = StatsQueue()
        queue.enqueue(StatsJob(name="features", path=Path("features.npy"), dtype="float32"))
        results = queue.flush(timeout=30.0)
        print(results["features"]["mean"])
    """

    def __init__(self) -> None:
        """Initialize the stats queue with a background worker thread."""
        self._job_queue: queue.Queue[StatsJob | None] = queue.Queue()
        self._results: dict[str, dict[str, Any] | None] = {}
        self._results_lock = threading.Lock()
        self._worker_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._start_worker()

    def _start_worker(self) -> None:
        """Start the background worker thread."""
        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()

    def _worker(self) -> None:
        """Background worker that processes jobs from the queue."""
        while not self._stop_event.is_set():
            try:
                # Use timeout to allow checking stop_event periodically
                job = self._job_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            # Sentinel value to stop worker
            if job is None:
                self._job_queue.task_done()
                break

            # Process the job
            try:
                stats = self._compute_stats(job)
                with self._results_lock:
                    self._results[job.name] = stats
            except Exception as e:
                logger.warning(f"Failed to compute stats for {job.name}: {e}")
                with self._results_lock:
                    # Skip failed jobs (don't add to results)
                    pass
            finally:
                self._job_queue.task_done()

    def _compute_stats(self, job: StatsJob) -> dict[str, Any] | None:
        """Compute statistics for a single job.

        Args:
            job: The stats job to process

        Returns:
            Dict with mean, std, min, max or None if file is missing/corrupt

        Raises:
            Exception: If file cannot be loaded or processed
        """
        if not job.path.exists():
            logger.warning(f"File not found: {job.path}")
            raise FileNotFoundError(f"File not found: {job.path}")

        try:
            # Load the numpy array using memory mapping (mmap_mode="r")
            # This allows accessing large files without loading them into RAM
            arr = np.load(job.path, mmap_mode="r")

            # Efficiently sample from the array without loading it all into RAM
            total_elements = arr.size
            if total_elements > job.sample_size:
                # Select random indices
                indices = np.random.choice(total_elements, job.sample_size, replace=False)
                # Slicing/indexing a memory-mapped array only loads selected chunks
                sample = arr.flat[indices]
            else:
                # Small enough to load
                sample = arr.flatten()

            # 1. Compute basic descriptive stats
            stats = {
                "mean": float(np.mean(sample)),
                "std": float(np.std(sample)),
                "min": float(np.min(sample)),
                "max": float(np.max(sample)),
                "samples_used": len(sample),
                "total_elements": total_elements,
            }

            # 2. Compute extended mechanistic stats
            # Entropy
            entropy_res = check_entropy(sample, min_entropy=0.0)
            if entropy_res.details:
                stats["entropy"] = entropy_res.details["entropy"]

            # Null ratio
            null_res = check_null_ratio(sample, max_null_ratio=1.0)
            if null_res.details:
                stats["null_ratio"] = null_res.details["null_ratio"]

            # Vocab utilization (only if vocab_size provided)
            if job.vocab_size is not None:
                vocab_res = check_vocab_utilization(sample, vocab_size=job.vocab_size, min_utilization=0.0)
                if vocab_res.details:
                    stats["vocab_utilization"] = vocab_res.details["utilization"]
                    stats["unique_count"] = vocab_res.details["unique_tokens"]

            return stats

        except Exception as e:
            logger.warning(f"Failed to load or process {job.path}: {e}")
            raise

    def enqueue(self, job: StatsJob) -> None:
        """Enqueue a stats job for processing (non-blocking).

        This method returns immediately without loading or processing the file.
        The job is added to the queue and will be processed by the background worker.

        Args:
            job: The stats job to enqueue (contains path reference, not data)
        """
        self._job_queue.put(job)

    def flush(self, timeout: float = 30.0) -> dict[str, dict[str, Any] | None]:
        """Block until all enqueued jobs are processed and return results.

        This method waits for the queue to be empty (all jobs processed) up to
        the specified timeout. Uses proper queue.join() semantics for correctness.

        Args:
            timeout: Maximum time to wait in seconds (0 = return immediately)

        Returns:
            Dict mapping job names to their stats dicts (mean, std, min, max).
            Jobs that failed are omitted from results.
            Returns empty dict if no jobs were enqueued or second flush.
        """
        if timeout == 0.0:
            # Return immediately with whatever we have
            with self._results_lock:
                results = self._results.copy()
                self._results.clear()
            return results

        # Use proper queue.join() semantics with timeout
        # Wrap join() in a thread since queue.join() doesn't support timeout directly
        done_event = threading.Event()

        def join_with_signal() -> None:
            self._job_queue.join()  # Blocks until all task_done() calls
            done_event.set()

        join_thread = threading.Thread(target=join_with_signal, daemon=True)
        join_thread.start()
        done_event.wait(timeout=timeout)

        # Return accumulated results and clear for next flush
        with self._results_lock:
            results = self._results.copy()
            self._results.clear()

        return results

    def shutdown(self) -> None:
        """Shutdown the worker thread gracefully."""
        self._stop_event.set()
        # Send sentinel to wake up worker
        self._job_queue.put(None)
        if self._worker_thread:
            self._worker_thread.join(timeout=1.0)

    def __del__(self) -> None:
        """Cleanup on deletion."""
        try:
            self.shutdown()
        except Exception:
            pass
