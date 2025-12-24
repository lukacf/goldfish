"""Security tests for metrics validation.

Tests for SQL injection, path traversal, and other security concerns.
"""

import pytest

from goldfish.validation import (
    InvalidArtifactPathError,
    InvalidMetricNameError,
    validate_artifact_path,
    validate_metric_name,
)


class TestMetricNameSQLInjection:
    """Test that metric names cannot be used for SQL injection."""

    def test_sql_injection_drop_table_rejected(self) -> None:
        """'; DROP TABLE metrics; -- should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("'; DROP TABLE run_metrics; --")

    def test_sql_injection_union_rejected(self) -> None:
        """UNION SELECT injection should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss UNION SELECT * FROM audit")

    def test_sql_injection_delete_rejected(self) -> None:
        """DELETE injection should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss; DELETE FROM stage_runs")

    def test_sql_injection_semicolon_rejected(self) -> None:
        """Semicolons (statement separator) should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss;")

    def test_sql_injection_comment_allowed_but_safe(self) -> None:
        """SQL comment syntax is allowed - parameterized queries prevent injection."""
        # Note: -- is allowed in metric names because:
        # 1. It's valid for names like "train--v2"
        # 2. SQL injection is prevented by parameterized queries, not name validation
        validate_metric_name("loss--comment")  # Should not raise

    def test_sql_injection_quotes_rejected(self) -> None:
        """SQL quotes should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss'or'1'='1")


class TestArtifactPathTraversal:
    """Test that artifact paths cannot be used for path traversal."""

    def test_path_traversal_simple(self) -> None:
        """Simple path traversal should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("../secret.txt")

    def test_path_traversal_nested(self) -> None:
        """Nested path traversal should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("models/../../../etc/passwd")

    def test_path_traversal_encoded(self) -> None:
        """Encoded path traversal should be rejected."""
        # Note: This test assumes the validation doesn't URL-decode
        # If someone passes URL-encoded .., it will still contain ".."
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("..%2F..%2Fetc/passwd")  # Still contains ..

    def test_path_traversal_encoded_dots(self) -> None:
        """URL-encoded dots should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("%2e%2e/%2e%2e/etc/passwd")

    def test_path_traversal_unicode_dots(self) -> None:
        """Unicode dot variants should be rejected."""
        # Fullwidth dots (U+FF0E)
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("\uff0e\uff0e/secret.txt")

    def test_path_traversal_double_slash(self) -> None:
        """Double slash path manipulation should be caught."""
        # This is more about normalization than traversal
        validate_artifact_path("models//valid.pt")  # Double slash is OK

    def test_path_traversal_backslash_windows(self) -> None:
        """Windows-style path traversal should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("..\\..\\windows\\system32")

    def test_absolute_path_unix(self) -> None:
        """Absolute Unix paths should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("/etc/passwd")

    def test_absolute_path_root(self) -> None:
        """Root path should be rejected."""
        with pytest.raises(InvalidArtifactPathError):
            validate_artifact_path("/")

    def test_symlink_traversal_attempt(self) -> None:
        """Symlink-like names are OK (actual symlink checking is elsewhere)."""
        # The validation doesn't check for actual symlinks, just path strings
        validate_artifact_path("symlink_to_secret.txt")  # Name is fine


class TestMetricNameShellInjection:
    """Test that metric names cannot be used for shell injection."""

    def test_shell_command_injection(self) -> None:
        """Command substitution should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss$(whoami)")

    def test_shell_backtick_injection(self) -> None:
        """Backtick command substitution should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss`id`")

    def test_shell_pipe_injection(self) -> None:
        """Pipe injection should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss|cat /etc/passwd")

    def test_shell_redirect_injection(self) -> None:
        """Redirect injection should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss > /tmp/evil")

    def test_shell_background_injection(self) -> None:
        """Background execution should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss & rm -rf /")

    def test_shell_newline_injection(self) -> None:
        """Newline injection should be rejected."""
        with pytest.raises(InvalidMetricNameError):
            validate_metric_name("loss\nrm -rf /")


class TestEdgeCasesValid:
    """Test edge cases that should be valid."""

    def test_valid_unicode_ascii_only(self) -> None:
        """Valid ASCII names should work."""
        validate_metric_name("loss")
        validate_metric_name("train_loss")
        validate_metric_name("epoch/loss")

    def test_valid_numbers_not_leading(self) -> None:
        """Numbers in names (not leading) should work."""
        validate_metric_name("layer1_loss")
        validate_metric_name("epoch0/loss")

    def test_valid_colons_for_namespacing(self) -> None:
        """Colons should work for namespacing."""
        validate_metric_name("wandb:loss")
        validate_metric_name("step:100/loss")

    def test_valid_dots_for_hierarchy(self) -> None:
        """Dots should work for hierarchy."""
        validate_metric_name("model.loss")
        validate_metric_name("layer.0.weight")
