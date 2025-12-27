# GEMINI.md - Project Context for Goldfish

## Project Overview
**Goldfish** is an ML experimentation platform designed as an **MCP (Model Context Protocol) server** to empower AI agents (like Claude Code) to conduct reliable, stateful, and long-running machine learning experiments. It solves the "goldfish memory" problem of agents by providing:
- **Isolated Workspaces**: Sandboxed environments for experiments.
- **Automatic Versioning**: Every run is backed by an immutable git-tagged snapshot (hidden from the agent).
- **Pipeline Workflows**: YAML-defined DAGs of stages with typed data signals.
- **Infrastructure Abstraction**: Agents write pure Python; Goldfish handles Docker, GCS, and GCE.
- **Pre-Run Review (SVS)**: Automated code review to catch bugs before execution.

### Key Technologies
- **Language**: Python 3.11+
- **Framework**: FastMCP (Model Context Protocol)
- **Execution**: Docker (Local), Google Compute Engine (Cloud)
- **Storage**: SQLite (Metadata), Google Cloud Storage (Large artifacts)
- **Versioning**: Git (Hidden backend engine)
- **Validation**: Pydantic, Ruff, Mypy, Claude Agent SDK (for reviews)

---

## Building and Running

### Prerequisites
- Python 3.11+
- Docker (for local execution)
- `uv` (recommended for dependency management)
- `pre-commit` (for linting hooks)

### Setup Commands
```bash
# Install dependencies in editable mode
pip install -e ".[dev]"

# Install pre-commit hooks (REQUIRED)
make install-hooks
```

### Key Commands
- **Serve (MCP)**: `goldfish serve` or `uv run goldfish serve`
- **Initialize**: `goldfish init` (runs in your ML repo)
- **Linting**: `make lint` (runs Ruff and Mypy)
- **Tests**:
    - `make test`: Fast unit tests (<1s)
    - `make test-unit`: All unit tests with coverage
    - `make test-integration`: Integration tests (~2min)
    - `make ci`: Full suite (lint + unit + integration)
    - `make test-deluxe`: Real GCE execution tests (requires cloud auth)

---

## Development Conventions

### 1. Test-Driven Development (TDD)
This project strictly follows TDD. **Write tests BEFORE implementation.**
- **Unit Tests**: `tests/unit/` (mocked, fast).
- **Integration Tests**: `tests/integration/` (real DB/Git).
- **Naming**: `test_<what>_<condition>_<expected>` (e.g., `test_mount_nonexistent_workspace_fails`).

### 2. Code Quality & Linting
- **No Suppressions**: Never use `# type: ignore` or ruff `noqa` unless absolutely necessary (and justified). Fix the source issue.
- **Strict Typing**: All functions must have type hints. Use `cast()` for database TypedDict returns.
- **Ruff + Mypy**: Automated via `make lint` and `pre-commit`.

### 3. Architecture Invariants
- **Database**: Use context managers (`with self.db._conn() as conn:`) for all transactions to ensure auto-commit/rollback.
- **Error Handling**: Use semantic errors from `src/goldfish/errors.py`. Never expose raw Git or OS errors to the MCP client.
- **Provenance**: Every `run()` call triggers an automatic sync and commit in the background dev repository before execution.
- **Security**: 
    - Always validate paths using `validate_path_within_root`.
    - Block symlinks to prevent TOCTOU attacks.
    - Sanitize all workspace and version names.

### 4. Six Abstractions
1. **Workspaces**: Isolated directories (copy-based mount).
2. **Versions**: Git tags representing 100% provenance.
3. **Pipelines**: YAML DAG definitions.
4. **Stages**: Isolated Python modules running in containers.
5. **Signals**: Typed data connectors (`npy`, `csv`, `directory`, `file`).
6. **Pre-Run Review**: AI-driven static analysis (SVS) before execution.

---

## File Structure Reference
- `src/goldfish/server.py`: MCP server entry point and tool registrations.
- `src/goldfish/jobs/stage_executor.py`: The "engine" - handles sync, versioning, review, and execution.
- `src/goldfish/db/database.py`: SQLite metadata layer.
- `src/goldfish/workspace/manager.py`: Workspace lifecycle management.
- `src/goldfish/infra/`: Execution backends (Docker, GCE).
- `src/goldfish/server_tools/`: Implementation of 40+ MCP tools.
