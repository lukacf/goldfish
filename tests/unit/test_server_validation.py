"""Tests for validation functions.

Verifies that validation module works correctly.
"""

import pytest

from goldfish.validation import (
    InvalidScriptPathError,
    InvalidSlotNameError,
    InvalidSourceNameError,
    InvalidWorkspaceNameError,
    validate_script_path,
    validate_slot_name,
    validate_source_name,
    validate_workspace_name,
)


class TestValidationFunctions:
    """Test validation functions."""

    def test_validation_imports_work(self):
        """Verify validation functions work correctly."""
        # Should not raise
        validate_workspace_name("valid-name")
        validate_source_name("valid-source")
        validate_script_path("scripts/train.py")
        validate_slot_name("w1", ["w1", "w2", "w3"])

    def test_workspace_name_validation(self):
        """Verify workspace name validation rejects invalid names."""
        with pytest.raises(InvalidWorkspaceNameError):
            validate_workspace_name("test; whoami")

    def test_source_name_validation(self):
        """Verify source name validation rejects invalid names."""
        with pytest.raises(InvalidSourceNameError):
            validate_source_name("../../../etc")

    def test_script_path_validation(self):
        """Verify script path validation rejects invalid paths."""
        with pytest.raises(InvalidScriptPathError):
            validate_script_path("/etc/passwd")

    def test_slot_name_validation(self):
        """Verify slot name validation rejects invalid slots."""
        with pytest.raises(InvalidSlotNameError):
            validate_slot_name("w99", ["w1", "w2", "w3"])
