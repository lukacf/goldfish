# Dockerfile for deluxe E2E test with Claude Code + MCP
# Tests full integration: Claude Code → MCP → Goldfish → GCE

FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH="/root/google-cloud-sdk/bin:${PATH}"

# Install Claude Code CLI (if available) or MCP SDK for client
RUN pip install mcp anthropic

# Set working directory
WORKDIR /workspace

# Copy Goldfish source
COPY . /goldfish
WORKDIR /goldfish

# Install Goldfish with MCP server
RUN pip install -e .

# Install test dependencies
RUN pip install pytest pytest-asyncio

# Create test workspace
RUN mkdir -p /test-workspace

WORKDIR /test-workspace

# Copy MCP client configuration
COPY tests/deluxe/mcp_config.json /root/.config/claude/

# Entry point runs the orchestrator
ENTRYPOINT ["python", "/goldfish/tests/deluxe/mcp_client_orchestrator.py"]
