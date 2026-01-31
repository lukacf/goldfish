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
import runpy
import threading
import time
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


def _reset_monitor_state() -> None:
    """Reset the monitor state (for testing only).

    This function should ONLY be called by tests. It's not part of the
    public API and should never be used in production code.
    """
    global _during_run_monitor, _during_run_monitor_started
    _during_run_monitor = None
    _during_run_monitor_started = False


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

# Global during-run monitor instance (lazily initialized)
_during_run_monitor = None
_during_run_monitor_lock = threading.Lock()
_during_run_monitor_started = False


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


def _ensure_monitor_started() -> None:
    """Ensure the during-run monitor is started (lazy initialization).

    This is called automatically when the user calls goldfish.io functions
    like load_input(), runtime_log(), etc. No need for explicit wrapper.

    Thread-safe and idempotent - safe to call multiple times.
    """
    global _during_run_monitor, _during_run_monitor_started

    # Fast path: already started
    if _during_run_monitor_started:
        return

    # Check if during-run is enabled
    if not _during_run_enabled():
        return

    with _during_run_monitor_lock:
        # Double-check after acquiring lock
        if _during_run_monitor_started:
            return

        try:
            from goldfish.svs.during_run_monitor import DuringRunMonitor

            outputs_dir = Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))
            _during_run_monitor = DuringRunMonitor(_load_svs_config(), outputs_dir)
            _during_run_monitor.start()
            _during_run_monitor_started = True
            logger.info("During-run AI monitor started automatically")
        except Exception as e:
            logger.error(f"Failed to start during-run monitor: {e}")
            _during_run_monitor_started = True  # Don't retry on failure


def _stop_monitor() -> None:
    """Stop the during-run monitor if running."""
    global _during_run_monitor

    if _during_run_monitor is not None:
        try:
            _during_run_monitor.stop(timeout=10.0)
            logger.debug("During-run monitor stopped")
        except Exception as e:
            logger.error(f"Failed to stop during-run monitor: {e}")


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


def _load_svs_config():
    """Load SVSConfig from environment (best-effort)."""
    import sys

    from goldfish.svs.config import SVSConfig

    config_json = os.environ.get("GOLDFISH_SVS_CONFIG")
    if not config_json:
        print("[SVS-DEBUG] No GOLDFISH_SVS_CONFIG env var, using defaults", file=sys.stderr, flush=True)
        return SVSConfig()
    try:
        config = SVSConfig.model_validate_json(config_json)
        print(
            f"[SVS-DEBUG] Loaded config: enabled={config.enabled}, during_run={config.ai_during_run_enabled}",
            file=sys.stderr,
            flush=True,
        )
        return config
    except Exception as exc:
        print(f"[SVS-DEBUG] Failed to parse config: {exc}", file=sys.stderr, flush=True)
        logger.warning(f"Failed to parse GOLDFISH_SVS_CONFIG: {exc}")
        return SVSConfig()


def _get_agent_provider(provider_name: str):
    """Instantiate an SVS agent provider by name."""
    from goldfish.svs.agent import get_agent_provider

    return get_agent_provider(provider_name)


def _run_post_run_review(stats: dict[str, dict[str, Any] | None]) -> None:
    """Run post-run AI review inside container (best-effort)."""
    from goldfish.svs.post_run import run_post_run_review

    config = _load_svs_config()
    if not config.enabled or not config.ai_post_run_enabled:
        return

    outputs_dir = Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))
    agent = _get_agent_provider(config.agent_provider)
    run_post_run_review(outputs_dir=outputs_dir, stats=stats, config=config, agent=agent)


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

    # Stop during-run monitor first (before any other cleanup)
    try:
        _stop_monitor()
    except Exception as e:
        logger.error(f"Error stopping during-run monitor: {e}")

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

        # Run post-run AI review inside container (best-effort)
        try:
            _run_post_run_review(stats)
        except Exception as e:
            logger.error(f"Failed to run post-run review: {e}")

        logger.debug("SVS stats flushed and manifest written successfully")
    except Exception as e:
        # Log error but NEVER raise - we must not mask original exceptions
        try:
            logger.error(f"Failed to flush SVS stats: {e}")
        except Exception:
            # Even logging can fail - swallow everything
            pass


def _get_outputs_dir() -> Path:
    """Get outputs directory (configurable for testing)."""
    return Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))


def _during_run_enabled() -> bool:
    """Check if during-run AI monitoring is enabled."""
    config = _load_svs_config()
    return config.enabled and getattr(config, "ai_during_run_enabled", False)


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
    global _during_run_monitor, _during_run_monitor_started

    # Register atexit handler BEFORE running module_main
    # This ensures finalization on SIGTERM or normal process exit
    atexit.register(_svs_finalize)

    # Write SVS context to disk if provided via env var
    svs_context_json = os.environ.get("GOLDFISH_SVS_CONTEXT")
    if svs_context_json:
        try:
            outputs_dir = _get_outputs_dir()
            goldfish_dir = outputs_dir / ".goldfish"
            goldfish_dir.mkdir(parents=True, exist_ok=True)
            (goldfish_dir / "svs_context.json").write_text(svs_context_json)
        except Exception as e:
            logger.error(f"Failed to write SVS context to disk: {e}")

    # Start during-run monitor if enabled (deferred to avoid fork issues)
    # Use global _during_run_monitor to coordinate with _ensure_monitor_started()
    import sys as _sys  # Local import to avoid shadowing

    print("[SVS-DEBUG] run_stage_with_svs() called", file=_sys.stderr, flush=True)

    try:
        during_run_enabled = _during_run_enabled()
        print(f"[SVS-DEBUG] _during_run_enabled() = {during_run_enabled}", file=_sys.stderr, flush=True)
        if during_run_enabled:
            outputs_dir = _get_outputs_dir()

            # Some test fixtures mock + reload modules in ways that can leave
            # `_during_run_monitor_started=True` even when there's no live monitor
            # (or when it points at a different outputs_dir). Ensure we always
            # have a live monitor for this run.
            monitor_to_stop = None
            need_start = False
            with _during_run_monitor_lock:
                existing = _during_run_monitor
                if existing is None:
                    need_start = True
                elif not isinstance(existing, threading.Thread):
                    need_start = True
                elif not existing.is_alive():
                    need_start = True
                else:
                    existing_outputs_dir = getattr(existing, "outputs_dir", None)
                    if existing_outputs_dir is not None and Path(existing_outputs_dir) != outputs_dir:
                        need_start = True

                if need_start:
                    monitor_to_stop = existing
                    _during_run_monitor = None
                    _during_run_monitor_started = False
                else:
                    _during_run_monitor_started = True

            if monitor_to_stop:
                try:
                    monitor_to_stop.stop(timeout=10.0)
                except Exception as e:
                    logger.error(f"Failed to stop during-run monitor: {e}")

            if need_start:
                try:
                    from goldfish.svs.during_run_monitor import DuringRunMonitor

                    # Small delay to let ML frameworks initialize (avoids issues with fork/multiprocessing)
                    print("[SVS-DEBUG] Starting during-run monitor...", file=_sys.stderr, flush=True)
                    time.sleep(1.0)
                    config = _load_svs_config()
                    print(
                        f"[SVS-DEBUG] Creating DuringRunMonitor with outputs_dir={outputs_dir}",
                        file=_sys.stderr,
                        flush=True,
                    )
                    new_monitor = DuringRunMonitor(config, outputs_dir)
                    new_monitor.start()
                    with _during_run_monitor_lock:
                        _during_run_monitor = new_monitor
                        _during_run_monitor_started = True
                    print("[SVS-DEBUG] During-run monitor started successfully", file=_sys.stderr, flush=True)
                    logger.info("During-run SVS monitor started")
                except Exception as e:
                    import traceback

                    print(f"[SVS-DEBUG] Failed to start during-run monitor: {e}", file=_sys.stderr, flush=True)
                    traceback.print_exc(file=_sys.stderr)
                    logger.error(f"Failed to start during-run monitor: {e}")
                    _during_run_monitor_started = True  # Don't retry on failure

        # Run user's main function
        exit_code = module_main()

        # Treat None as success (0)
        if exit_code is None:
            exit_code = 0

        return exit_code

    finally:
        # Stop monitor if started (using global monitor)
        monitor_to_stop = _during_run_monitor
        _during_run_monitor = None
        _during_run_monitor_started = False

        if monitor_to_stop:
            try:
                monitor_to_stop.stop(timeout=10.0)
            except Exception as e:
                logger.error(f"Failed to stop during-run monitor: {e}")

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


def run_module_with_svs(module_name: str) -> int:
    """Run a Python module via runpy with SVS finalization."""

    def _run_module() -> None:
        runpy.run_module(module_name, run_name="__main__")

    return run_stage_with_svs(_run_module)
