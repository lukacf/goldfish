#!/bin/bash
# Setup Claude Code to use Goldfish MCP server

set -e

echo "Setting up Claude Code with Goldfish MCP server..."

# Create Claude Code config directory
mkdir -p ~/.config/claude

# Add Goldfish MCP server to Claude Code
# Using the goldfish project directory in /workspace
cat > ~/.config/claude/mcp.json <<EOF
{
  "mcpServers": {
    "goldfish": {
      "command": "uv",
      "args": [
        "--directory",
        "/goldfish",
        "run",
        "goldfish",
        "serve",
        "--project",
        "/workspace"
      ]
    }
  }
}
EOF

echo "✓ Claude Code configured with Goldfish MCP server"
echo ""
echo "MCP Configuration:"
cat ~/.config/claude/mcp.json
echo ""
