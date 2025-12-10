# Dockerfile for deluxe E2E test
# Tests Claude Code using Goldfish MCP server with real GCE execution

FROM python:3.12-slim

# Install system dependencies including Docker CLI
RUN apt-get update && apt-get install -y \
    git \
    curl \
    nodejs \
    npm \
    jq \
    ca-certificates \
    gnupg \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI (for building and pushing images via host Docker daemon)
RUN install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli docker-buildx-plugin && \
    rm -rf /var/lib/apt/lists/*

# Install Google Cloud SDK (system-wide so testuser can access it)
RUN curl -sSL https://sdk.cloud.google.com | bash -s -- --install-dir=/opt && \
    chmod -R a+rX /opt/google-cloud-sdk
ENV PATH="/opt/google-cloud-sdk/bin:${PATH}"

# Install uv (Python package manager used by Claude Code)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/opt/google-cloud-sdk/bin:/root/.local/bin:${PATH}"

# Configure git (required for Goldfish workspace operations)
RUN git config --global user.email "deluxe-test@goldfish.local" && \
    git config --global user.name "Goldfish Deluxe Test"

# Install Claude Code CLI
# Note: This assumes claude-code is available via npm
# Alternative: Use Anthropic API with tool use if Claude Code CLI isn't available
RUN npm install -g @anthropic-ai/claude-code || echo "Claude Code CLI not available, will use API"

# Create non-root user for running tests (Claude Code requires non-root for --dangerously-skip-permissions)
RUN useradd -m -u 1000 -s /bin/bash testuser && \
    mkdir -p /ml-project-test-repo && \
    chown -R testuser:testuser /ml-project-test-repo

WORKDIR /goldfish

# Copy Goldfish source
COPY . /goldfish

# Install Goldfish
RUN uv pip install --system -e .

# Create test repo directory (NOT called "workspace" to avoid confusion with Goldfish workspace concept)
RUN mkdir -p /ml-project-test-repo

WORKDIR /ml-project-test-repo

# Copy test scripts and make them executable by everyone
COPY tests/deluxe/setup_claude.sh /usr/local/bin/setup_claude.sh
COPY tests/deluxe/run_test.sh /usr/local/bin/run_test.sh
RUN chmod 755 /usr/local/bin/setup_claude.sh /usr/local/bin/run_test.sh

# Make goldfish directory accessible to testuser
RUN chown -R testuser:testuser /goldfish

# Switch to non-root user
USER testuser

# Install uv for testuser and update PATH
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/opt/google-cloud-sdk/bin:/home/testuser/.local/bin:${PATH}"

# Configure git for testuser
RUN git config --global user.email "deluxe-test@goldfish.local" && \
    git config --global user.name "Goldfish Deluxe Test"

# Entry point sets up Claude Code and runs test
ENTRYPOINT ["/usr/local/bin/run_test.sh"]
