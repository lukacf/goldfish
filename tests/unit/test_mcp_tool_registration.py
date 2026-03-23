"""Regression test: all @mcp.tool() functions must be imported in server.py.

The @mcp.tool() decorator registers the function with FastMCP, but only if
the module is imported. server.py has explicit imports for each tool module.
If a new tool is added to a server_tools module but not imported in server.py,
it appears in the tool schema but raises "Unknown tool" when called.
"""

import ast
import importlib
from pathlib import Path


def _get_mcp_tool_names(module_path: Path) -> list[str]:
    """Extract function names decorated with @mcp.tool() from a Python file."""
    tree = ast.parse(module_path.read_text())
    tool_names = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            # Match @mcp.tool() — an ast.Call whose func is ast.Attribute
            if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
                attr = decorator.func
                if isinstance(attr.value, ast.Name) and attr.value.id == "mcp" and attr.attr == "tool":
                    tool_names.append(node.name)
    return tool_names


def _get_server_imports() -> set[str]:
    """Get all names imported in server.py from server_tools modules."""
    server_path = Path(importlib.util.find_spec("goldfish.server").origin)  # type: ignore[union-attr]
    tree = ast.parse(server_path.read_text())
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "server_tools" in node.module:
            for alias in node.names:
                imported.add(alias.name)
    return imported


def test_all_mcp_tools_imported_in_server():
    """Every @mcp.tool() in server_tools/ must be imported in server.py.

    REGRESSION: goldfish_version was added to utility_tools.py with @mcp.tool()
    but not imported in server.py. The tool appeared in the schema but raised
    "Unknown tool" when called.
    """
    server_tools_dir = Path(importlib.util.find_spec("goldfish.server_tools").origin).parent  # type: ignore[union-attr]
    server_imports = _get_server_imports()

    missing: list[str] = []
    for py_file in sorted(server_tools_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        tool_names = _get_mcp_tool_names(py_file)
        for name in tool_names:
            # Skip private helpers (e.g., _get_run_svs_findings_tool)
            if name.startswith("_"):
                continue
            if name not in server_imports:
                missing.append(f"{py_file.name}:{name}")

    assert not missing, f"@mcp.tool() functions not imported in server.py (will raise 'Unknown tool'): {missing}"
