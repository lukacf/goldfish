# Dockerfile for deluxe E2E test
# Tests Claude Code using Goldfish MCP server with real GCE execution

FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    nodejs \
    npm \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH="/root/google-cloud-sdk/bin:${PATH}"

# Install uv (Python package manager used by Claude Code)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Install Claude Code CLI
# Note: This assumes claude-code is available via npm
# Alternative: Use Anthropic API with tool use if Claude Code CLI isn't available
RUN npm install -g @anthropic-ai/claude-code || echo "Claude Code CLI not available, will use API"

WORKDIR /goldfish

# Copy Goldfish source
COPY . /goldfish

# Install Goldfish
RUN uv pip install --system -e .

# Create workspace directory
RUN mkdir -p /workspace

WORKDIR /workspace

# Copy test scripts
COPY tests/deluxe/setup_claude.sh /usr/local/bin/setup_claude.sh
COPY tests/deluxe/run_test.sh /usr/local/bin/run_test.sh
RUN chmod +x /usr/local/bin/*.sh

# Entry point sets up Claude Code and runs test
ENTRYPOINT ["/usr/local/bin/run_test.sh"]
