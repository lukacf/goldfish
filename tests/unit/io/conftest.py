"""Fixtures for IO tests."""

from __future__ import annotations

import pytest

from goldfish.io.bootstrap import _reset_finalized_flag


@pytest.fixture(autouse=True)
def reset_bootstrap_state() -> None:
    """Reset bootstrap module state before each test.

    This ensures tests don't interfere with each other by leaving
    the finalization flag set to True.
    """
    _reset_finalized_flag()
