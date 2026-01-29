# Contributing to Goldfish

Thank you for your interest in contributing! This guide is designed for human developers to help you navigate the codebase and submit your first PR effectively.

## Table of Contents
- [For First-Time Contributors](#for-first-time-contributors)
- [Development Setup](#development-setup)
- [Git Workflow](#git-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guide](#testing-guide)
- [Key Subsystems & Files](#key-subsystems--files)
- [PR Process](#pr-process)

---

## For First-Time Contributors

If you're new to Goldfish, we recommend starting with these well-scoped areas:

1.  **Validation Logic** (`src/goldfish/validation.py`): Improve input sanitization or regex patterns for workspace/version names.
2.  **Utility Tools** (`src/goldfish/server_tools/utility_tools.py`): Add simple helper tools that provide better visibility into project state for AI agents.
3.  **Documentation Snippets**: Add real-world ML examples to `SKILL.md` or improve the tutorial in `end_to_end_example.md`.
4.  **Unit Test Gaps**: Look for missing edge cases in `tests/unit/` to help us reach 100% coverage on core logic.

---

## Development Setup

### Prerequisites
- **Python 3.11+**
- **Docker Desktop** (required for local execution tests)
- **uv** (recommended for fast dependency management)

### Quick Setup
```bash
# Clone and enter the repo
git clone https://github.com/lukacf/goldfish.git
cd goldfish

# Install in editable mode with all dev dependencies
uv pip install -e ".[dev]"

# Install pre-commit hooks (REQUIRED for linting)
make install-hooks

# Verify installation
make ci
```

---

## Git Workflow

We follow a standard fork-and-pull-request workflow:

1.  **Fork** the repository on GitHub.
2.  **Clone** your fork and add the original as `upstream`:
    ```bash
    git remote add upstream https://github.com/lukacf/goldfish.git
    ```
3.  **Create a Branch**: Use a descriptive prefix like `feat/`, `fix/`, or `docs/`.
    ```bash
    git checkout -b feat/add-gpu-metrics
    ```
4.  **Make Changes** and run tests frequently.
5.  **Push and PR**: Push to your fork and open a PR against our `main` branch.

---

## Coding Standards

Goldfish prioritizes type safety and defensive programming. For a deep dive into internal invariants and architecture, always refer to **[CLAUDE.md](CLAUDE.md)**.

### Type Hinting
All function signatures must have type hints. Use `None` explicitly for optional returns.
```python
def get_slot_info(slot_id: str) -> SlotInfo | None:
    ...
```

### Semantic Error Handling
Never raise generic `Exception`. Use or add specific error types in `src/goldfish/errors.py`.
```python
if not workspace_exists:
    raise WorkspaceNotFoundError(f"Workspace {name} not found")
```

### Audit Logging
Any operation that modifies state or project configuration must record an entry in the audit log.
```python
self.db.log_audit(operation="delete_workspace", workspace=name, reason=reason)
```

---

## Testing Guide

We use **Test-Driven Development (TDD)**. Write your failing test *before* the fix/feature.

### Which Test Should I Write?

| Scenario | Test Type | Location |
| :--- | :--- | :--- |
| Pure logic, regex, or math (no DB/Git) | **Unit** | `tests/unit/` |
| Requires database queries or Git operations | **Integration** | `tests/integration/` |
| Full end-to-end container execution | **E2E** | `tests/e2e/` |

### Code Examples

**Unit Test (Fast, Isolated):**
```python
def test_validate_workspace_name_valid():
    from goldfish.validation import validate_workspace_name
    # No mocks needed for pure logic
    assert validate_workspace_name("my-exp_v1") is None
```

**Integration Test (Real DB/Git):**
```python
def test_db_record_audit(test_db):
    # Use the test_db fixture from conftest.py
    test_db.log_audit("test_op", "user_reason")
    with test_db._conn() as conn:
        row = conn.execute("SELECT * FROM audit").fetchone()
        assert row["operation"] == "test_op"
```

---

## Key Subsystems & Files

Use this map to find the right place for your changes:

| Subsystem | Primary Files | Common Tasks |
| :--- | :--- | :--- |
| **MCP Tools** | `src/goldfish/server_tools/` | Add/Modify tools available to AI agents. |
| **Execution Engine** | `src/goldfish/jobs/stage_executor.py` | Change how stages are executed (uses RunBackend protocol). |
| **Cloud Backends** | `src/goldfish/cloud/adapters/` | Add new compute backends (implement RunBackend, ObjectStorage). See [CLOUD_ABSTRACTION.md](docs/CLOUD_ABSTRACTION.md). |
| **Validation (SVS)** | `src/goldfish/svs/` | Add new mechanistic checks (entropy, variance, etc.). |
| **Database** | `src/goldfish/db/database.py` | Update schema or add complex metadata queries. |
| **Workspace Ops** | `src/goldfish/workspace/manager.py` | Modify how local slots and git branches sync. |
| **Container IO** | `src/goldfish/io/` | Update the library used inside user containers. |

---

## PR Process

1.  **Open an Issue**: For non-trivial changes, discuss the design with maintainers first.
2.  **Lint & Check**: Run `make lint` and `make ci` locally. We do not accept PRs with linting errors or failing tests.
3.  **Documentation**: If you add a tool, update `tools_reference.md` and `SKILL.md`.
4.  **Description**: Use the PR template. Clearly state the **Why** and the **Architectural Impact**.
5.  **Review**: Address reviewer feedback promptly. Once approved and CI passes, a maintainer will merge your work.
