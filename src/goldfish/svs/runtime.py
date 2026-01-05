"""Runtime helpers for during-run SVS monitoring."""

import os
from pathlib import Path


def should_stop() -> bool:
    """Check if SVS requested early termination of the current stage.

    Users should call this in their training loops to support AI-driven
    auto-stop of failing experiments.

    Example:
        for epoch in range(epochs):
            train_one_epoch()
            if goldfish.io.should_stop():
                print("SVS requested stop. Cleaning up...")
                break
    """
    outputs_dir = Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))
    stop_file = outputs_dir / ".goldfish" / "stop_requested"
    return stop_file.exists()


def get_stop_reason() -> str | None:
    """Get the reason for the stop request, if any.

    Returns:
        The reason message written by SVS, or None if no stop requested.
    """
    outputs_dir = Path(os.environ.get("GOLDFISH_OUTPUTS_DIR", "/mnt/outputs"))
    stop_file = outputs_dir / ".goldfish" / "stop_requested"
    if not stop_file.exists():
        return None

    try:
        return stop_file.read_text().strip()
    except Exception:
        return "Unknown reason (failed to read stop_requested file)"
