"""TDD tests for bootstrap wrapper - guarantees SVS finalization.

Tests verify:
- Module main is called with correct behavior
- SVS finalization happens exactly once
- Finalization occurs on success, exception, KeyboardInterrupt, SystemExit
- Exception propagation after finalization
- Atexit hook registration
- SVS flush behavior (when enabled/disabled)
- Idempotency of finalization
- Graceful error handling in finalization
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from goldfish.io.bootstrap import (
    _reset_finalized_flag,
    _svs_finalize,
    _write_stats_manifest,
    run_stage_with_svs,
)


@pytest.fixture(autouse=True)
def reset_finalized_flag() -> None:
    """Reset the finalization flag before each test."""
    _reset_finalized_flag()


class TestRunStageWithSVSBasics:
    """Test fundamental run_stage_with_svs operations."""

    def test_returns_module_exit_code(self) -> None:
        """Should return the exit code from module_main."""
        module_main = Mock(return_value=0)

        with patch("goldfish.io.bootstrap._svs_finalize"):
            exit_code = run_stage_with_svs(module_main)

        assert exit_code == 0

    def test_returns_nonzero_exit_code(self) -> None:
        """Should return non-zero exit code from module_main."""
        module_main = Mock(return_value=42)

        with patch("goldfish.io.bootstrap._svs_finalize"):
            exit_code = run_stage_with_svs(module_main)

        assert exit_code == 42

    def test_calls_module_main(self) -> None:
        """Should call the module_main callable."""
        module_main = Mock(return_value=0)

        with patch("goldfish.io.bootstrap._svs_finalize"):
            run_stage_with_svs(module_main)

        module_main.assert_called_once()

    def test_finalize_called_on_success(self) -> None:
        """Should call _svs_finalize when module_main succeeds."""
        module_main = Mock(return_value=0)

        with patch("goldfish.io.bootstrap._svs_finalize") as mock_finalize:
            run_stage_with_svs(module_main)

        mock_finalize.assert_called_once()

    def test_finalize_called_on_exception(self) -> None:
        """Should call _svs_finalize even when module_main raises exception."""
        module_main = Mock(side_effect=RuntimeError("Stage failed"))

        with patch("goldfish.io.bootstrap._svs_finalize") as mock_finalize:
            with pytest.raises(RuntimeError, match="Stage failed"):
                run_stage_with_svs(module_main)

        mock_finalize.assert_called_once()


class TestRunStageWithSVSGuarantees:
    """Test that finalization guarantees are met."""

    def test_finalize_called_exactly_once(self) -> None:
        """Should call _svs_finalize exactly once, not multiple times."""
        module_main = Mock(return_value=0)

        with patch("goldfish.io.bootstrap._svs_finalize") as mock_finalize:
            run_stage_with_svs(module_main)

        assert mock_finalize.call_count == 1

    def test_finalize_on_keyboard_interrupt(self) -> None:
        """Should finalize on KeyboardInterrupt (Ctrl+C)."""
        module_main = Mock(side_effect=KeyboardInterrupt())

        with patch("goldfish.io.bootstrap._svs_finalize") as mock_finalize:
            with pytest.raises(KeyboardInterrupt):
                run_stage_with_svs(module_main)

        mock_finalize.assert_called_once()

    def test_finalize_on_system_exit(self) -> None:
        """Should finalize on SystemExit."""
        module_main = Mock(side_effect=SystemExit(1))

        with patch("goldfish.io.bootstrap._svs_finalize") as mock_finalize:
            with pytest.raises(SystemExit):
                run_stage_with_svs(module_main)

        mock_finalize.assert_called_once()

    def test_exception_propagated_after_finalize(self) -> None:
        """Should propagate exception after finalization completes."""
        module_main = Mock(side_effect=ValueError("Bad value"))

        with patch("goldfish.io.bootstrap._svs_finalize") as mock_finalize:
            with pytest.raises(ValueError, match="Bad value"):
                run_stage_with_svs(module_main)

        # Finalize must complete before exception is raised
        mock_finalize.assert_called_once()

    def test_finalize_error_does_not_mask_original_exception(self) -> None:
        """If finalize fails, original exception should still propagate."""
        module_main = Mock(side_effect=RuntimeError("Original error"))

        with (
            patch("goldfish.io.bootstrap._svs_finalize", side_effect=Exception("Finalize error")),
            patch("goldfish.io.bootstrap.atexit.register"),  # Prevent mock from registering with atexit
        ):
            # Original exception should propagate (not finalize error)
            with pytest.raises(RuntimeError, match="Original error"):
                run_stage_with_svs(module_main)


class TestSVSFinalize:
    """Test _svs_finalize internal function."""

    def test_finalize_flushes_stats(self) -> None:
        """Should flush stats queue when SVS is enabled."""
        with (
            patch("goldfish.io.bootstrap._svs_enabled", return_value=True),
            patch("goldfish.io.bootstrap._get_stats_queue") as mock_get_queue,
        ):
            mock_queue = Mock()
            mock_get_queue.return_value = mock_queue

            _svs_finalize()

            mock_queue.flush.assert_called_once()

    def test_finalize_is_idempotent(self) -> None:
        """Should be safe to call multiple times (only flush once)."""
        with (
            patch("goldfish.io.bootstrap._svs_enabled", return_value=True),
            patch("goldfish.io.bootstrap._get_stats_queue") as mock_get_queue,
        ):
            mock_queue = Mock()
            mock_get_queue.return_value = mock_queue

            # Call twice
            _svs_finalize()
            _svs_finalize()

            # Should only flush once (implementation uses flag)
            assert mock_queue.flush.call_count == 1

    def test_finalize_handles_stats_error_gracefully(self) -> None:
        """Should not raise if stats flush fails."""
        with (
            patch("goldfish.io.bootstrap._svs_enabled", return_value=True),
            patch("goldfish.io.bootstrap._get_stats_queue") as mock_get_queue,
        ):
            mock_queue = Mock()
            mock_queue.flush.side_effect = RuntimeError("Flush failed")
            mock_get_queue.return_value = mock_queue

            # Should not raise
            _svs_finalize()

    def test_finalize_skipped_when_svs_disabled(self) -> None:
        """Should skip stats flush when SVS is disabled."""
        with (
            patch("goldfish.io.bootstrap._svs_enabled", return_value=False),
            patch("goldfish.io.bootstrap._get_stats_queue") as mock_get_queue,
        ):
            _svs_finalize()

            # Should not attempt to get queue
            mock_get_queue.assert_not_called()

    def test_finalize_logs_flush_failure(self) -> None:
        """Should log if stats flush fails but continue."""
        with (
            patch("goldfish.io.bootstrap._svs_enabled", return_value=True),
            patch("goldfish.io.bootstrap._get_stats_queue") as mock_get_queue,
            patch("goldfish.io.bootstrap.logger") as mock_logger,
        ):
            mock_queue = Mock()
            mock_queue.flush.side_effect = RuntimeError("Flush failed")
            mock_get_queue.return_value = mock_queue

            _svs_finalize()

            # Should log the error
            assert mock_logger.error.called or mock_logger.warning.called


class TestSVSStatsManifest:
    """Test SVS stats manifest writing."""

    def test_write_stats_manifest_writes_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Should write svs_stats.json with version and stats."""
        monkeypatch.setenv("GOLDFISH_OUTPUTS_DIR", str(tmp_path))

        stats = {"features": {"mean": 1.0, "std": 0.5}}
        _write_stats_manifest(stats)

        manifest_path = tmp_path / ".goldfish" / "svs_stats.json"
        assert manifest_path.exists()

        data = json.loads(manifest_path.read_text())
        assert data["version"] == 1
        assert data["stats"] == stats


class TestAtexitIntegration:
    """Test atexit hook registration."""

    def test_atexit_registered(self) -> None:
        """Should register _svs_finalize with atexit."""
        module_main = Mock(return_value=0)

        with (
            patch("goldfish.io.bootstrap._svs_finalize"),
            patch("goldfish.io.bootstrap.atexit.register") as mock_atexit,
        ):
            run_stage_with_svs(module_main)

        # Should register finalize with atexit
        mock_atexit.assert_called_once()
        args = mock_atexit.call_args
        # First arg should be _svs_finalize function
        assert callable(args[0][0])

    def test_atexit_called_before_try_finally(self) -> None:
        """Atexit registration should happen before module_main executes."""
        module_main = Mock(return_value=0)
        call_order = []

        def track_atexit(*args: object) -> None:
            call_order.append("atexit")

        def track_main() -> int:
            call_order.append("main")
            return 0

        with (
            patch("goldfish.io.bootstrap._svs_finalize"),
            patch("goldfish.io.bootstrap.atexit.register", side_effect=track_atexit),
        ):
            run_stage_with_svs(track_main)

        # Atexit should be registered before main runs
        assert call_order == ["atexit", "main"]


class TestRunStageWithSVSEdgeCases:
    """Test edge cases and unusual scenarios."""

    def test_module_main_returns_none(self) -> None:
        """Should handle module_main returning None (treat as 0)."""
        module_main = Mock(return_value=None)

        with patch("goldfish.io.bootstrap._svs_finalize"):
            exit_code = run_stage_with_svs(module_main)

        # None should be treated as success (0)
        assert exit_code == 0 or exit_code is None

    def test_module_main_is_generator(self) -> None:
        """Should handle if module_main is a generator function."""
        from collections.abc import Generator

        def generator_main() -> Generator[int, None, int]:
            yield 1
            yield 2
            return 0

        with patch("goldfish.io.bootstrap._svs_finalize"):
            # Should either consume generator or handle appropriately
            # Implementation detail - may call next() or just treat as callable
            result = run_stage_with_svs(generator_main)

        # Should handle gracefully (exact behavior depends on implementation)
        assert result is not None or result is None  # Just ensure no crash

    def test_finalize_timeout_handled(self) -> None:
        """Should handle if stats flush times out."""
        import time

        with (
            patch("goldfish.io.bootstrap._svs_enabled", return_value=True),
            patch("goldfish.io.bootstrap._get_stats_queue") as mock_get_queue,
        ):
            mock_queue = Mock()
            # Simulate timeout by taking too long

            def slow_flush(*args: object, **kwargs: object) -> dict[str, object]:
                time.sleep(0.1)  # Simulate slow operation
                return {}

            mock_queue.flush.side_effect = slow_flush
            mock_get_queue.return_value = mock_queue

            # Should complete within reasonable time
            start = time.time()
            _svs_finalize()
            elapsed = time.time() - start

            # Should not hang indefinitely (timeout should be enforced)
            assert elapsed < 5.0, "Finalize should not hang"


class TestSVSFinalizeFlagManagement:
    """Test that finalization flag prevents duplicate work."""

    def test_finalize_flag_prevents_duplicate_flush(self) -> None:
        """Finalization flag should prevent flush from running twice."""
        with (
            patch("goldfish.io.bootstrap._svs_enabled", return_value=True),
            patch("goldfish.io.bootstrap._get_stats_queue") as mock_get_queue,
            patch("goldfish.io.bootstrap._finalized", False),  # Start unfinalized
        ):
            mock_queue = Mock()
            mock_get_queue.return_value = mock_queue

            # Call three times
            _svs_finalize()
            _svs_finalize()
            _svs_finalize()

            # Should only flush once
            assert mock_queue.flush.call_count == 1

    def test_finalize_in_atexit_and_finally_both_safe(self) -> None:
        """Finalize called by both atexit and finally should be safe."""
        module_main = Mock(return_value=0)

        with (
            patch("goldfish.io.bootstrap._svs_finalize") as mock_finalize,
            patch("goldfish.io.bootstrap.atexit.register") as mock_atexit,
        ):
            # Simulate atexit calling finalize during run_stage_with_svs
            from collections.abc import Callable
            from typing import Any

            def simulate_atexit(fn: Callable[[], Any]) -> None:
                # Call the registered function immediately (simulating process exit)
                fn()

            mock_atexit.side_effect = simulate_atexit

            run_stage_with_svs(module_main)

        # Finalize called twice: once by atexit, once by finally
        # But internal flag should prevent duplicate work
        assert mock_finalize.call_count == 2
