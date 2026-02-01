"""Test that cloud/factory.py can be imported in container environment.

Container images only have a subset of goldfish packages:
- goldfish.io, goldfish.metrics, goldfish.svs, goldfish.utils, goldfish.cloud
- goldfish.validation, goldfish.errors, goldfish.config

NOT available in containers:
- goldfish.infra (Docker builder, metadata bus)
- goldfish.db (database)
- goldfish.server, goldfish.server_tools
- goldfish.workspace, goldfish.jobs, goldfish.pipeline

This test ensures the cloud/factory.py module can be imported when
goldfish.infra is unavailable, which is critical for save_checkpoint()
and other container-side storage operations.
"""

import subprocess
import sys
from pathlib import Path

# Resolve project root dynamically for portable tests
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


class TestContainerImports:
    """Tests for container-compatible imports."""

    def test_create_storage_from_env_does_not_require_goldfish_infra(self):
        """create_storage_from_env should work without goldfish.infra.

        Regression test for bug where save_checkpoint() crashed with:
        ModuleNotFoundError: No module named 'goldfish.infra'

        The fix was to make LocalMetadataBus import lazy (inside create_signal_bus)
        instead of top-level in factory.py.

        Uses subprocess to fully isolate the import test.
        """
        # Use subprocess for clean import isolation
        code = """
import sys

# Block goldfish.infra entirely BEFORE any imports
class InfraBlocker:
    def find_module(self, name, path=None):
        if name.startswith("goldfish.infra"):
            return self
        return None
    def load_module(self, name):
        raise ImportError(f"{name} not available in container")

sys.meta_path.insert(0, InfraBlocker())

# Now try importing - this will fail if goldfish.infra is needed at top level
try:
    from goldfish.cloud.factory import create_storage_from_env
    print("SUCCESS")
except ImportError as e:
    print(f"FAILED: {e}")
    sys.exit(1)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "SUCCESS" in result.stdout

    def test_factory_top_level_imports_are_container_safe(self):
        """Verify factory.py doesn't import goldfish.infra at module level.

        This is a static check that goldfish.infra is not in the direct
        import dependencies of factory.py.
        """
        import ast
        from pathlib import Path

        factory_path = Path(__file__).parent.parent.parent.parent / "src" / "goldfish" / "cloud" / "factory.py"
        source = factory_path.read_text()
        tree = ast.parse(source)

        # Collect all top-level imports
        infra_imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if "goldfish.infra" in alias.name:
                        infra_imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module and "goldfish.infra" in node.module:
                    # Check if it's inside a function (allowed) vs top-level (not allowed)
                    # Top-level imports have col_offset typically 0
                    # We check if node is at module level by seeing if it's in the body
                    if any(node is child for child in tree.body):
                        infra_imports.append(node.module)

        assert not infra_imports, (
            f"goldfish.infra imports at module level in factory.py: {infra_imports}. "
            "These must be moved inside functions for container compatibility."
        )

    def test_io_save_checkpoint_import_chain_subprocess(self):
        """The full import chain for save_checkpoint should be container-safe.

        Uses subprocess for clean import isolation.
        """
        code = """
import sys

class InfraBlocker:
    def find_module(self, name, path=None):
        if name.startswith("goldfish.infra"):
            return self
        return None
    def load_module(self, name):
        raise ImportError(f"{name} not available in container")

sys.meta_path.insert(0, InfraBlocker())

try:
    from goldfish.io import save_checkpoint
    print("SUCCESS")
except ImportError as e:
    print(f"FAILED: {e}")
    sys.exit(1)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "SUCCESS" in result.stdout
