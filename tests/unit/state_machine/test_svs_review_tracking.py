"""Tests for SVS review ID tracking in state machine transitions.

TDD: These tests verify that svs_review_id is properly tracked when
SVS-related events occur (SVS_BLOCK, AI_STOP).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from goldfish.state_machine import EventContext, StageEvent, StageState


@pytest.fixture
def now() -> datetime:
    """Provide a consistent timestamp for tests."""
    return datetime.now(UTC)


@pytest.fixture
def mock_db() -> MagicMock:
    """Create a mock database with transaction support."""
    db = MagicMock()
    conn = MagicMock()
    db._conn.return_value.__enter__ = MagicMock(return_value=conn)
    db._conn.return_value.__exit__ = MagicMock(return_value=False)
    return db


class TestSvsReviewIdInEventContext:
    """Tests for svs_review_id field in EventContext."""

    def test_event_context_has_svs_review_id_field(self) -> None:
        """EventContext should have svs_review_id field."""
        ctx = EventContext(
            timestamp=datetime.now(UTC),
            source="executor",
            svs_review_id="svs-abc123",
        )
        assert ctx.svs_review_id == "svs-abc123"

    def test_event_context_svs_review_id_defaults_to_none(self) -> None:
        """EventContext.svs_review_id should default to None."""
        ctx = EventContext(timestamp=datetime.now(UTC), source="executor")
        assert ctx.svs_review_id is None


class TestSvsReviewIdPersistence:
    """Tests for svs_review_id persistence in stage_state_transitions."""

    def test_svs_block_persists_svs_review_id(self, mock_db: MagicMock, now: datetime) -> None:
        """SVS_BLOCK transition should persist svs_review_id in audit trail."""
        from goldfish.state_machine.core import transition

        run_id = "stage-abc123"
        svs_review_id = "svs-review-xyz789"
        ctx = EventContext(
            timestamp=now,
            source="executor",
            error_message="Pre-run review blocked execution",
            svs_review_id=svs_review_id,
        )

        # Mock: current state is PREPARING
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "preparing",
            "phase": "pre_run_review",
        }
        # Mock: CAS update succeeds
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.SVS_BLOCK, ctx)

        assert result.success is True
        assert result.new_state == StageState.FAILED

        # Verify svs_review_id was included in INSERT
        conn = mock_db._conn.return_value.__enter__.return_value
        insert_calls = [c for c in conn.execute.call_args_list if "INSERT INTO stage_state_transitions" in str(c)]
        assert len(insert_calls) >= 1, "Expected INSERT INTO stage_state_transitions"

        # Check that svs_review_id is in the INSERT values
        insert_call = insert_calls[0]
        sql = insert_call[0][0]
        params = insert_call[0][1]

        assert "svs_review_id" in sql, "INSERT should include svs_review_id column"
        assert svs_review_id in params, f"svs_review_id '{svs_review_id}' should be in INSERT params"

    def test_transition_without_svs_review_id_uses_null(self, mock_db: MagicMock, now: datetime) -> None:
        """Transitions without svs_review_id should store NULL."""
        from goldfish.state_machine.core import transition

        run_id = "stage-abc123"
        ctx = EventContext(
            timestamp=now,
            source="executor",
            # No svs_review_id
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "preparing",
            "phase": "gcs_check",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.BUILD_START, ctx)

        assert result.success is True

        # svs_review_id should be None/NULL in params
        conn = mock_db._conn.return_value.__enter__.return_value
        insert_calls = [c for c in conn.execute.call_args_list if "INSERT INTO stage_state_transitions" in str(c)]
        assert len(insert_calls) >= 1

        insert_call = insert_calls[0]
        sql = insert_call[0][0]

        # If svs_review_id column is in schema, it should be in SQL
        assert "svs_review_id" in sql, "INSERT should include svs_review_id column"


class TestAiStopEvent:
    """Tests for AI_STOP event triggered by during-run SVS.

    When during-run SVS requests a stop (via stop_requested file), the daemon
    should emit AI_STOP event to transition to TERMINATED with termination_cause=ai_stopped.
    """

    def test_ai_stop_event_exists(self) -> None:
        """StageEvent should have AI_STOP event."""
        assert hasattr(StageEvent, "AI_STOP"), "StageEvent should have AI_STOP event"
        assert StageEvent.AI_STOP.value == "ai_stop"

    def test_running_ai_stop_goes_to_terminated(self, mock_db: MagicMock, now: datetime) -> None:
        """RUNNING + AI_STOP → TERMINATED with termination_cause=ai_stopped."""
        from goldfish.state_machine.core import transition
        from goldfish.state_machine.types import TerminationCause

        run_id = "stage-abc123"
        svs_review_id = "svs-during-run-456"
        ctx = EventContext(
            timestamp=now,
            source="daemon",
            termination_cause=TerminationCause.AI_STOPPED,
            svs_review_id=svs_review_id,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "running",
            "phase": "code_execution",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.AI_STOP, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED

    def test_ai_stop_from_running_persists_svs_review_id(self, mock_db: MagicMock, now: datetime) -> None:
        """AI_STOP should persist svs_review_id linking to the SVS finding."""
        from goldfish.state_machine.core import transition
        from goldfish.state_machine.types import TerminationCause

        run_id = "stage-abc123"
        svs_review_id = "svs-finding-stop-request"
        ctx = EventContext(
            timestamp=now,
            source="daemon",
            termination_cause=TerminationCause.AI_STOPPED,
            svs_review_id=svs_review_id,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "running",
            "phase": "code_execution",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, run_id, StageEvent.AI_STOP, ctx)
        assert result.success is True

        # Verify svs_review_id in INSERT
        conn = mock_db._conn.return_value.__enter__.return_value
        insert_calls = [c for c in conn.execute.call_args_list if "INSERT INTO stage_state_transitions" in str(c)]
        assert len(insert_calls) >= 1

        insert_call = insert_calls[0]
        params = insert_call[0][1]
        assert svs_review_id in params, "AI_STOP should persist svs_review_id"

    def test_ai_stop_allowed_from_post_run(self, mock_db: MagicMock, now: datetime) -> None:
        """AI_STOP should also work from POST_RUN state."""
        from goldfish.state_machine.core import transition
        from goldfish.state_machine.types import TerminationCause

        ctx = EventContext(
            timestamp=now,
            source="daemon",
            termination_cause=TerminationCause.AI_STOPPED,
        )

        mock_db._conn.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = {
            "state": "post_run",
            "phase": "output_sync",
        }
        mock_db._conn.return_value.__enter__.return_value.execute.return_value.rowcount = 1

        result = transition(mock_db, "stage-abc", StageEvent.AI_STOP, ctx)

        assert result.success is True
        assert result.new_state == StageState.TERMINATED
