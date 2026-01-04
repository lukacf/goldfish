"""Integration test for SVS MCP tool registration."""

import inspect

import pytest


@pytest.mark.asyncio
async def test_svs_tools_registered_on_server():
    """Verify that SVS tools are actually registered with the FastMCP instance."""
    # Importing server triggers tool registration via imports
    from goldfish.server import mcp

    # FastMCP stores tools in its internal registry
    # Use _list_tools() which is the standard way to inspect a FastMCP instance
    sig = inspect.signature(mcp._list_tools)
    if len(sig.parameters) > 0:
        tools = await mcp._list_tools(None)
    else:
        tools = await mcp._list_tools()
    tool_names = [tool.name for tool in tools]

    expected_svs_tools = [
        "manage_patterns",
        "get_run_svs_findings",
    ]

    for expected in expected_svs_tools:
        assert expected in tool_names, f"Tool {expected} not found in registered tools. Found: {tool_names}"
