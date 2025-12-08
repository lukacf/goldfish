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

# Add Goldfish MCP server using claude mcp add command
# This properly registers the server with Claude Code
# Use GOLDFISH_START_DIR to tell the server where to start (since uv --directory changes CWD)
claude mcp add --transport stdio goldfish -- bash -c 'GOLDFISH_START_DIR=/ml-project-test-repo uv --directory /goldfish run goldfish serve'

echo "✓ Claude Code configured with Goldfish MCP server"
echo ""

# Verify configuration
echo "MCP Configuration:"
claude mcp list
echo ""
