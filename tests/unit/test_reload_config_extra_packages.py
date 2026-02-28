"""Regression test: reload_config() must update extra_packages for image inspect.

Bug: BaseImageManager singleton held old config after reload_config() was called.
Changes to extra_packages in goldfish.yaml were not reflected in image inspect.

Fix: Call _reset_base_image_manager() in reload_config() to invalidate the singleton.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

from goldfish.server_tools import infra_tools


def test_reset_base_image_manager_clears_singleton():
    """_reset_base_image_manager() must clear the singleton so new config is used.

    This is a regression test for the bug where extra_packages changes
    in goldfish.yaml were not picked up by image inspect after reload_config().
    """
    # Simulate having an existing manager singleton
    old_manager = MagicMock()
    infra_tools._base_image_manager = old_manager

    # Reset should clear the singleton
    infra_tools._reset_base_image_manager()

    assert infra_tools._base_image_manager is None


def test_get_base_image_manager_creates_new_after_reset():
    """After reset, _get_base_image_manager() creates a new manager with fresh config.

    This test verifies the end-to-end behavior: after reset, the next call
    to _get_base_image_manager() creates a new manager with fresh config.
    """
    # Reset the manager to start clean
    infra_tools._reset_base_image_manager()

    # Create mock context with specific config
    mock_config = MagicMock()
    mock_config.docker.extra_packages = {"base": ["old-package==1.0"]}
    mock_context = MagicMock()
    mock_context.project_root = Path("/fake/project")
    mock_context.config = mock_config
    mock_context.db = MagicMock()

    with (
        patch("goldfish.server_tools.infra_tools.has_context", return_value=True),
        patch("goldfish.server_tools.infra_tools.get_context", return_value=mock_context),
    ):
        # Get the manager - should create new one with mock config
        manager1 = infra_tools._get_base_image_manager()

        # Verify it has the config
        assert manager1.docker_config == mock_config.docker

        # Now simulate config change - create new config with different packages
        new_config = MagicMock()
        new_config.docker.extra_packages = {"base": ["new-package==2.0"]}
        mock_context.config = new_config

        # Without reset, should return same manager (cached singleton)
        manager2 = infra_tools._get_base_image_manager()
        assert manager2 is manager1  # Same instance
        assert manager2.docker_config == mock_config.docker  # Old config!

        # After reset, should create new manager with new config
        infra_tools._reset_base_image_manager()
        manager3 = infra_tools._get_base_image_manager()
        assert manager3 is not manager1  # New instance
        assert manager3.docker_config == new_config.docker  # New config!

    # Clean up
    infra_tools._reset_base_image_manager()


def test_reload_config_calls_reset_base_image_manager():
    """reload_config() must call _reset_base_image_manager().

    Verifies the fix is in place by checking the source contains the call.
    """
    import inspect

    from goldfish.server_tools.utility_tools import reload_config

    # Get the source code of the function
    source = inspect.getsource(reload_config)

    # Verify the import and reset call are in the function body
    assert (
        "from goldfish.server_tools.infra_tools import _reset_base_image_manager" in source
    ), "reload_config() must import _reset_base_image_manager"
    assert "_reset_base_image_manager()" in source, (
        "reload_config() must call _reset_base_image_manager() to invalidate "
        "the BaseImageManager singleton after config reload"
    )
