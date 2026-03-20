"""Tests for workspace improvements: from_ref, branch_workspace, configurable slots."""

from __future__ import annotations


class TestConfigurableSlots:
    def test_slots_default_is_three(self) -> None:
        from goldfish.config import GoldfishConfig

        config = GoldfishConfig(project_name="test", dev_repo_path="test-dev")
        assert config.slots == ["w1", "w2", "w3"]

    def test_slots_accepts_integer(self) -> None:
        """slots: 5 should generate [w1, w2, w3, w4, w5]."""
        from goldfish.config import GoldfishConfig

        config = GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=5)
        assert config.slots == ["w1", "w2", "w3", "w4", "w5"]

    def test_slots_accepts_explicit_list(self) -> None:
        """slots: [dev, staging, prod] should work."""
        from goldfish.config import GoldfishConfig

        config = GoldfishConfig(
            project_name="test",
            dev_repo_path="test-dev",
            slots=["dev", "staging", "prod"],
        )
        assert config.slots == ["dev", "staging", "prod"]

    def test_slots_integer_one(self) -> None:
        from goldfish.config import GoldfishConfig

        config = GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=1)
        assert config.slots == ["w1"]

    def test_slots_integer_ten(self) -> None:
        from goldfish.config import GoldfishConfig

        config = GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=10)
        assert len(config.slots) == 10
        assert config.slots[9] == "w10"

    def test_slots_rejects_boolean_true(self) -> None:
        """slots: true must not be silently treated as slots: 1."""
        import pytest
        from pydantic import ValidationError

        from goldfish.config import GoldfishConfig

        with pytest.raises(ValidationError):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=True)

    def test_slots_rejects_boolean_false(self) -> None:
        """slots: false must not be silently treated as slots: 0."""
        import pytest
        from pydantic import ValidationError

        from goldfish.config import GoldfishConfig

        with pytest.raises(ValidationError):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=False)

    def test_slots_rejects_zero(self) -> None:
        """slots: 0 must be rejected — project needs at least one slot."""
        import pytest
        from pydantic import ValidationError

        from goldfish.config import GoldfishConfig

        with pytest.raises((ValidationError, ValueError)):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=0)

    def test_slots_rejects_negative(self) -> None:
        """slots: -1 must be rejected."""
        import pytest
        from pydantic import ValidationError

        from goldfish.config import GoldfishConfig

        with pytest.raises((ValidationError, ValueError)):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=-3)


class TestFromRefValidation:
    def test_validate_from_ref_accepts_main(self) -> None:
        from goldfish.validation import validate_from_ref

        validate_from_ref("main")  # Should not raise

    def test_validate_from_ref_accepts_workspace_name(self) -> None:
        from goldfish.validation import validate_from_ref

        validate_from_ref("my-workspace")  # Should not raise

    def test_validate_from_ref_rejects_remote_refs(self) -> None:
        import pytest

        from goldfish.errors import GoldfishError
        from goldfish.validation import validate_from_ref

        with pytest.raises(GoldfishError):
            validate_from_ref("refs/remotes/origin/main")

    def test_validate_from_ref_rejects_empty_string(self) -> None:
        """Empty string must be rejected, not silently defaulted to main."""
        import pytest

        from goldfish.errors import GoldfishError
        from goldfish.validation import validate_from_ref

        with pytest.raises(GoldfishError):
            validate_from_ref("")
