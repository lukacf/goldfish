#!/bin/bash
# Quick test to see if Claude Code can see Goldfish MCP tools

set -e

echo "Setting up Claude Code..."
/usr/local/bin/setup_claude.sh

echo ""
echo "Testing if Claude Code can see MCP tools..."
echo ""

# Ask Claude to list available MCP tools
claude -p --dangerously-skip-permissions "List all available MCP tools that start with 'mcp__goldfish'. For each tool, show its name and what it does."

echo ""
echo "Test complete"
