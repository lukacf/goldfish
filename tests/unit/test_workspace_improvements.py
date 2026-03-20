"""Tests for workspace improvements: configurable slots, workspace branching."""

from __future__ import annotations

import pytest
from pydantic import ValidationError


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
        from goldfish.config import GoldfishConfig

        config = GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=["dev", "staging", "prod"])
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
        from goldfish.config import GoldfishConfig

        with pytest.raises(ValidationError):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=True)

    def test_slots_rejects_boolean_false(self) -> None:
        from goldfish.config import GoldfishConfig

        with pytest.raises(ValidationError):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=False)

    def test_slots_rejects_zero(self) -> None:
        from goldfish.config import GoldfishConfig

        with pytest.raises((ValidationError, ValueError)):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=0)

    def test_slots_rejects_negative(self) -> None:
        from goldfish.config import GoldfishConfig

        with pytest.raises((ValidationError, ValueError)):
            GoldfishConfig(project_name="test", dev_repo_path="test-dev", slots=-3)


class TestCreateWorkspaceBranching:
    """Test the Goldfish-level branching API (no raw git refs)."""

    def test_from_version_without_workspace_is_invalid(self) -> None:
        """from_version requires from_workspace."""
        from unittest.mock import MagicMock, patch

        from goldfish.errors import GoldfishError
        from goldfish.workspace.manager import WorkspaceManager

        manager = MagicMock()
        manager.git.branch_exists.return_value = False
        # Call the real create_workspace method
        real_method = WorkspaceManager.create_workspace

        with pytest.raises(GoldfishError, match="from_version requires from_workspace"):
            with patch("goldfish.workspace.manager.validate_reason"):
                real_method(
                    manager,
                    "new-ws",
                    "goal",
                    "Testing version branching without workspace",
                    from_version="v3",
                )
