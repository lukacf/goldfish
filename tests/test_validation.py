"""Tests for input validation - P0 Security.

These tests verify that user input is properly sanitized to prevent:
- Command injection via workspace names
- Path traversal via script paths
- Shell metacharacter injection

TDD: Write failing tests first, then implement validation.py
"""

import pytest

from goldfish.validation import (
    validate_workspace_name,
    validate_source_name,
    validate_script_path,
    validate_slot_name,
    InvalidWorkspaceNameError,
    InvalidSourceNameError,
    InvalidScriptPathError,
    InvalidSlotNameError,
)


class TestWorkspaceNameValidation:
    """Test workspace name validation against command injection."""

    # Valid workspace names
    @pytest.mark.parametrize("name", [
        "fix-bug",
        "feature_123",
        "abc",
        "a",
        "test-feature-branch",
        "v1",
        "experiment-2024-12-04",
        "my_workspace_name",
        "CamelCaseName",
        "MixedCase-with_separators",
        "a" * 64,  # Max length
    ])
    def test_valid_workspace_names(self, name: str):
        """Valid workspace names should pass validation."""
        validate_workspace_name(name)  # Should not raise

    # Invalid: Command injection attempts
    @pytest.mark.parametrize("name,reason", [
        ("test; rm -rf /", "semicolon command separator"),
        ("foo|bar", "pipe"),
        ("foo`whoami`", "backticks"),
        ("$(whoami)", "command substitution"),
        ("foo & bar", "background operator"),
        ("foo && bar", "AND operator"),
        ("foo || bar", "OR operator"),
        ('foo"bar', "double quote"),
        ("foo'bar", "single quote"),
        ("foo\\bar", "backslash"),
        ("foo>bar", "redirect"),
        ("foo<bar", "redirect"),
        ("foo*", "glob"),
        ("foo?bar", "glob"),
        ("foo[bar]", "glob"),
        ("foo{bar}", "brace expansion"),
        ("foo$HOME", "variable expansion"),
        ("foo~bar", "tilde expansion"),
        ("foo!bar", "history expansion"),
        ("foo\nbar", "newline"),
        ("foo\tbar", "tab"),
    ])
    def test_rejects_command_injection(self, name: str, reason: str):
        """Workspace names with shell metacharacters should be rejected."""
        with pytest.raises(InvalidWorkspaceNameError) as exc_info:
            validate_workspace_name(name)
        assert "invalid" in str(exc_info.value).lower() or "character" in str(exc_info.value).lower()

    # Invalid: Path traversal attempts
    @pytest.mark.parametrize("name", [
        "../../../etc/passwd",
        "..\\..\\windows",
        "foo/../bar",
        "/absolute/path",
        "foo/bar/baz",  # No slashes allowed
    ])
    def test_rejects_path_traversal(self, name: str):
        """Workspace names with path components should be rejected."""
        with pytest.raises(InvalidWorkspaceNameError):
            validate_workspace_name(name)

    # Invalid: Edge cases
    @pytest.mark.parametrize("name,reason", [
        ("", "empty string"),
        (" ", "whitespace only"),
        ("  spaces  ", "leading/trailing spaces"),
        ("foo bar", "spaces in name"),
        ("-startswithdash", "starts with dash"),
        ("_startsunderscore", "starts with underscore"),
        ("endsdash-", "ends with dash"),
        ("endsunderscore_", "ends with underscore"),
        ("a" * 65, "too long"),
        (".", "single dot"),
        ("..", "double dot"),
        (".hidden", "starts with dot"),
    ])
    def test_rejects_invalid_edge_cases(self, name: str, reason: str):
        """Edge case invalid names should be rejected."""
        with pytest.raises(InvalidWorkspaceNameError):
            validate_workspace_name(name)


class TestSourceNameValidation:
    """Test source name validation - same rules as workspace names."""

    @pytest.mark.parametrize("name", [
        "eurusd-ticks",
        "synth_v11",
        "preprocessed",
        "raw_data_2024",
    ])
    def test_valid_source_names(self, name: str):
        """Valid source names should pass validation."""
        validate_source_name(name)

    @pytest.mark.parametrize("name", [
        "test; rm -rf /",
        "../../../etc",
        "",
        "foo bar",
    ])
    def test_rejects_invalid_source_names(self, name: str):
        """Invalid source names should be rejected."""
        with pytest.raises(InvalidSourceNameError):
            validate_source_name(name)


class TestScriptPathValidation:
    """Test script path validation against command injection and path traversal."""

    @pytest.mark.parametrize("path", [
        "scripts/train.py",
        "entrypoints/main.sh",
        "run.py",
        "scripts/data/preprocess.py",
        "test_runner.sh",
    ])
    def test_valid_script_paths(self, path: str):
        """Valid script paths should pass validation."""
        validate_script_path(path)

    @pytest.mark.parametrize("path,reason", [
        ("/etc/passwd", "absolute path"),
        ("/usr/bin/python", "absolute path"),
        ("../../../etc/passwd", "path traversal"),
        ("scripts/../../../etc/passwd", "embedded traversal"),
        ("foo; rm -rf /", "command injection"),
        ("foo|cat /etc/passwd", "pipe"),
        ("foo`whoami`.py", "backticks"),
        ("$(whoami).py", "command substitution"),
        ("", "empty"),
        ("scripts/foo.txt", "wrong extension"),
        ("scripts/foo", "no extension"),
        ("scripts/foo.pyc", "compiled python"),
        ("foo bar.py", "spaces"),
    ])
    def test_rejects_invalid_script_paths(self, path: str, reason: str):
        """Invalid script paths should be rejected."""
        with pytest.raises(InvalidScriptPathError):
            validate_script_path(path)


class TestSlotNameValidation:
    """Test slot name validation."""

    @pytest.mark.parametrize("slot,valid_slots", [
        ("w1", ["w1", "w2", "w3"]),
        ("w2", ["w1", "w2", "w3"]),
        ("w3", ["w1", "w2", "w3"]),
        ("slot1", ["slot1", "slot2"]),
    ])
    def test_valid_slot_names(self, slot: str, valid_slots: list[str]):
        """Valid slot names should pass validation."""
        validate_slot_name(slot, valid_slots)

    @pytest.mark.parametrize("slot,valid_slots,reason", [
        ("w4", ["w1", "w2", "w3"], "not in list"),
        ("../w1", ["w1", "w2", "w3"], "path traversal"),
        ("w1; rm -rf", ["w1", "w2", "w3"], "command injection"),
        ("", ["w1", "w2", "w3"], "empty"),
    ])
    def test_rejects_invalid_slot_names(self, slot: str, valid_slots: list[str], reason: str):
        """Invalid slot names should be rejected."""
        with pytest.raises(InvalidSlotNameError):
            validate_slot_name(slot, valid_slots)


class TestValidationErrorMessages:
    """Test that error messages are helpful but don't leak sensitive info."""

    def test_workspace_error_message_is_helpful(self):
        """Error message should explain what's wrong without exposing internals."""
        with pytest.raises(InvalidWorkspaceNameError) as exc_info:
            validate_workspace_name("test; whoami")

        error_msg = str(exc_info.value)
        # Should mention what's invalid
        assert "workspace" in error_msg.lower() or "name" in error_msg.lower()
        # Should NOT expose that it's about git branches
        assert "branch" not in error_msg.lower()
        assert "git" not in error_msg.lower()

    def test_script_error_message_is_helpful(self):
        """Script path error should explain the problem."""
        with pytest.raises(InvalidScriptPathError) as exc_info:
            validate_script_path("../../../etc/passwd")

        error_msg = str(exc_info.value)
        assert "script" in error_msg.lower() or "path" in error_msg.lower()
