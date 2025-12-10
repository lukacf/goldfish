#!/bin/bash
# Setup Claude Code to use Goldfish MCP server

set -e

echo "Setting up Claude Code with Goldfish MCP server..."

# Test if goldfish command works
echo "Testing goldfish command..."
uv --directory /goldfish run goldfish --help > /dev/null 2>&1 || {
    echo "ERROR: goldfish command failed"
    exit 1
}
echo "✓ Goldfish command works"
echo ""

# Register Goldfish MCP server for project /ml-project-test-repo
pushd /ml-project-test-repo >/dev/null
claude mcp add --transport stdio goldfish -- bash -c 'GOLDFISH_START_DIR=/ml-project-test-repo uv --directory /goldfish run goldfish serve'
popd >/dev/null

echo "✓ Claude Code configured with Goldfish MCP server"
echo ""

# Verify configuration
echo "MCP Configuration:"
pushd /ml-project-test-repo >/dev/null
claude mcp list
popd >/dev/null
echo ""
