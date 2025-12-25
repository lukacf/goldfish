"""Bootstrap wrapper for guaranteed SVS finalization.

This module wraps user's main() function to ensure SVS hooks are called
even on crash, using both atexit handlers and try/finally blocks for
defense-in-depth.

Key guarantees:
- SVS finalize runs exactly once (idempotent)
- Runs on success, exception, KeyboardInterrupt, SystemExit
- Original exceptions propagate after finalization
- Graceful error handling in finalization
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Flag to ensure finalization happens only once
_finalized: bool = False


def _reset_finalized_flag() -> None:
    """Reset the finalization flag (for testing only).

    This function should ONLY be called by tests. It's not part of the
    public API and should never be used in production code.
    """
    global _finalized
    _finalized = False


def _svs_enabled() -> bool:
    """Check if SVS stats collection is enabled.

    Reads from environment variable set by stage executor.

    Returns:
        True if SVS stats should be collected, False otherwise
    """
    # Check if stats collection is enabled via environment variable
    # This is set by the stage executor when launching containers
    svs_stats = os.environ.get("GOLDFISH_SVS_STATS_ENABLED", "false").lower()
    return svs_stats in ("true", "1", "yes")


# Global stats queue instance (lazily initialized)
_stats_queue = None
_stats_queue_lock = threading.Lock()


def _get_stats_queue():
    """Get the global stats queue instance (singleton).

    Returns:
        StatsQueue instance for async stats computation
    """
    global _stats_queue
    from goldfish.io.stats import StatsQueue

    if _stats_queue is not None:
        return _stats_queue

    with _stats_queue_lock:
        if _stats_queue is None:
            _stats_queue = StatsQueue()
        return _stats_queue


def _write_stats_manifest(stats: dict[str, dict[str, Any] | None]) -> None:
    """Write mechanistic stats to .goldfish/svs_stats.json.

    Args:
        stats: Aggregated stats from StatsQueue
    """
    outputs_dir = Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))
    goldfish_dir = outputs_dir / ".goldfish"

    try:
        goldfish_dir.mkdir(parents=True, exist_ok=True)

        manifest = {
            "version": 1,
            "stats": stats or {},
        }

        manifest_path = goldfish_dir / "svs_stats.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        logger.debug(f"Wrote SVS stats manifest to {manifest_path}")
    except Exception as e:
        logger.error(f"Failed to write SVS stats manifest: {e}")


def _svs_finalize() -> None:
    """Flush stats + cleanup. Called once via atexit/finally.

    This function is idempotent - safe to call multiple times.
    Only the first call will actually perform finalization.

    Handles errors gracefully to avoid masking original exceptions.
    """
    global _finalized

    # Guard against duplicate finalization
    if _finalized:
        return

    _finalized = True

    # Skip if SVS is disabled
    if not _svs_enabled():
        return

    # Flush stats queue if enabled
    # CRITICAL: Must not raise any exceptions to avoid masking original errors
    try:
        stats_queue = _get_stats_queue()
        # Capture the stats returned from flush()
        stats = stats_queue.flush(timeout=10.0)

        # Always write the manifest (even if empty stats)
        _write_stats_manifest(stats)

        logger.debug("SVS stats flushed and manifest written successfully")
    except Exception as e:
        # Log error but NEVER raise - we must not mask original exceptions
        try:
            logger.error(f"Failed to flush SVS stats: {e}")
        except Exception:
            # Even logging can fail - swallow everything
            pass


def run_stage_with_svs(module_main: Callable[[], int | None]) -> int:
    """Wrap user's main() with guaranteed SVS hooks.

    Uses both atexit handlers and try/finally blocks for defense-in-depth:
    - atexit: Catches SIGTERM, normal exit
    - finally: Catches exceptions, KeyboardInterrupt, SystemExit

    Args:
        module_main: User's main function to execute

    Returns:
        Exit code from module_main (0 for success, or None treated as 0)

    Raises:
        Any exception raised by module_main (after finalization)

    Example:
        def main() -> int:
            # User code here
            return 0

        if __name__ == "__main__":
            sys.exit(run_stage_with_svs(main))
    """
    # Register atexit handler BEFORE running module_main
    # This ensures finalization on SIGTERM or normal process exit
    atexit.register(_svs_finalize)

    try:
        # Run user's main function
        exit_code = module_main()

        # Treat None as success (0)
        if exit_code is None:
            exit_code = 0

        return exit_code

    finally:
        # Ensure finalization happens even on exception
        # This is defense-in-depth - finalize will be called by both
        # the finally block and potentially by atexit
        # The idempotency guard ensures it only runs once
        # Wrap in try/except to NEVER mask original exceptions
        try:
            _svs_finalize()
        except Exception:
            # Swallow any finalize errors to preserve original exception
            pass
