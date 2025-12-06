"""Integration tests for server-level input validation.

Verifies that validation is properly wired into MCP tools.
"""

import pytest

from goldfish.validation import (
    InvalidWorkspaceNameError,
    InvalidSourceNameError,
    InvalidScriptPathError,
    InvalidSlotNameError,
)


class TestServerValidationIntegration:
    """Test that server functions call validation."""

    def test_validation_imports_work(self):
        """Verify validation module imports work in server."""
        from goldfish.server import (
            validate_workspace_name,
            validate_source_name,
            validate_script_path,
            validate_slot_name,
        )

        # Should not raise
        validate_workspace_name("valid-name")
        validate_source_name("valid-source")
        validate_script_path("scripts/train.py")
        validate_slot_name("w1", ["w1", "w2", "w3"])

    def test_workspace_name_validation_in_server(self):
        """Verify workspace name validation is imported and available."""
        from goldfish.server import validate_workspace_name

        with pytest.raises(InvalidWorkspaceNameError):
            validate_workspace_name("test; whoami")

    def test_source_name_validation_in_server(self):
        """Verify source name validation is imported and available."""
        from goldfish.server import validate_source_name

        with pytest.raises(InvalidSourceNameError):
            validate_source_name("../../../etc")

    def test_script_path_validation_in_server(self):
        """Verify script path validation is imported and available."""
        from goldfish.server import validate_script_path

        with pytest.raises(InvalidScriptPathError):
            validate_script_path("/etc/passwd")

    def test_slot_name_validation_in_server(self):
        """Verify slot name validation is imported and available."""
        from goldfish.server import validate_slot_name

        with pytest.raises(InvalidSlotNameError):
            validate_slot_name("w99", ["w1", "w2", "w3"])
