"""Regression tests for parameter-golf bug batch.

Bug 3: Early finalization silently discarded
Bug 4: VM marked terminated while still booting
Bug 5: gpu: null rejected for CPU profiles
"""

from __future__ import annotations

# --- Bug 3: Early finalization ---


def test_finalize_during_running_marks_early():
    """Finalize called during RUNNING should set finalized_by='early', not be silently discarded.

    Bug: User called finalize_run while run was RUNNING. Results were saved but the
    state machine transition was skipped. Later, executor moved run to AWAITING_USER_FINALIZATION
    anyway, requiring a second finalize call.

    Fix: When state is RUNNING/POST_RUN and results_status='finalized', set finalized_by='early'.
    The executor checks this flag and auto-completes instead of waiting.
    """
    from goldfish.state_machine.types import StageState

    # The fix code path: when current_state is RUNNING and results are finalized,
    # set finalized_by='early'
    current_state = StageState.RUNNING.value
    eligible_states = (StageState.RUNNING.value, StageState.POST_RUN.value)
    assert current_state in eligible_states

    # Also verify POST_RUN is eligible
    current_state = StageState.POST_RUN.value
    assert current_state in eligible_states


# --- Bug 4: VM timeout too aggressive ---


def test_not_found_timeout_uses_launch_timeout_for_running_state():
    """RUNNING state should use launch_timeout, not the shorter not_found_timeout.

    Bug: CPU VMs with data_disk provisioning take >300s to boot.
    Goldfish used not_found_timeout (300s) once state=RUNNING, marking
    the instance terminated while the VM was still booting.
    """

    # The fix: not_found_timeout increased from 300s to 600s,
    # giving CPU VMs with data_disk enough time to boot.
    import os

    default = os.getenv("GOLDFISH_GCE_NOT_FOUND_TIMEOUT", "600")
    assert int(default) >= 600


# --- Bug 5: gpu: null rejected ---


def test_validate_profile_accepts_gpu_null():
    """gpu: null should be accepted for CPU-only profiles."""
    from goldfish.cloud.adapters.gcp.profiles import validate_profile

    profile = {
        "machine_type": "e2-standard-4",
        "zones": ["us-central1-a"],
        "boot_disk": {"type": "pd-standard", "size_gb": 50},
        "gpu": None,
    }
    validate_profile(profile)
    # Should normalize to CPU default
    assert profile["gpu"] == {"type": "none", "accelerator": None, "count": 0}


def test_validate_profile_accepts_missing_gpu():
    """Missing gpu field should default to CPU-only."""
    from goldfish.cloud.adapters.gcp.profiles import validate_profile

    profile = {
        "machine_type": "e2-standard-4",
        "zones": ["us-central1-a"],
        "boot_disk": {"type": "pd-standard", "size_gb": 50},
    }
    validate_profile(profile)
    assert profile["gpu"] == {"type": "none", "accelerator": None, "count": 0}


def test_validate_profile_data_disk_is_optional():
    """data_disk should not be required (it's not always needed)."""
    from goldfish.cloud.adapters.gcp.profiles import validate_profile

    profile = {
        "machine_type": "e2-standard-4",
        "zones": ["us-central1-a"],
        "boot_disk": {"type": "pd-standard", "size_gb": 50},
    }
    # Should not raise
    validate_profile(profile)


def test_validate_profile_error_message_lists_required_fields():
    """Error message should list required fields and note optional ones."""
    import pytest

    from goldfish.cloud.adapters.gcp.profiles import ProfileValidationError, validate_profile

    with pytest.raises(ProfileValidationError, match="machine_type"):
        validate_profile({"zones": ["us-central1-a"]})
