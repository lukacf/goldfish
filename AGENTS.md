# Repository Guidelines

## Project Structure & Module Organization
- Source: `src/goldfish/` (core: `jobs/`, `server_tools/`, `pipeline/`, `infra/`, `db/`).
- Tests: `tests/` (unit + e2e), deluxe GCE flow under `tests/deluxe/`.
- Config & schema: `goldfish.yaml`, `src/goldfish/db/schema.sql`.
- Docs: `docs/` and this guide.

## Build, Test, and Development Commands
- Install deps: `uv pip install --system -e .`
- Unit/integration tests: `pytest tests/test_stage_executor.py tests/test_pipeline_executor.py tests/test_e2e_pipeline_execution.py tests/test_e2e_workflows.py tests/test_lineage_manager.py tests/test_docker_builder.py`
- Deluxe GCE e2e: `cd tests/deluxe && docker compose run --rm deluxe-test` (uses `.env` for GCP settings).
- Lint (Python style/typing): run `ruff` / `mypy` if available in your environment (not enforced in CI yet).

## Coding Style & Naming Conventions
- Python: PEP8-ish, 4-space indent; prefer explicit imports; avoid `from module import *`.
- Models/DTOs in `models.py` use Pydantic; name responses `*Response`, info structs `*Info`.
- CAS/DB helpers live in `jobs/conversion.py` and `db/database.py`; keep DB access parameterized (no f-strings with values).
- Docker tags sanitized to lowercase; workspace/stage IDs use prefixes `stage-`, `prun-`.

## Testing Guidelines
- Tests co-located in `tests/`; e2e names start with `test_e2e_*`.
- Targeted test runs preferred (see command above); for new features add unit tests plus, when relevant, a happy-path e2e.
- Deluxe test is slow (GCE). Use sparingly before merge; ensure `.env` has valid GCP project/bucket.

## Commit & Pull Request Guidelines
- Commits: imperative present tense, scoped summaries (e.g., “Add CAS finalize guard”). Keep related changes together.
- PRs: clear summary + testing section; link issues when applicable. For runtime changes, note impact on GCE/local backends.
- Avoid committing logs or test artifacts (`tests/deluxe/*.log`, `.goldfish/` already gitignored).

## Security & Configuration Tips
- GCE backend requires `artifact_registry` in `goldfish.yaml`; env vars `GOLDFISH_GCE_PROJECT`, `GOLDFISH_GCS_BUCKET` auto-fill on init.
- Docker pushes need `gcloud auth configure-docker <registry>` (the code auto-checks auth but assumes `gcloud` is installed).
- Migrations run automatically; schema version tracked in SQLite. Fresh DB lives under `.goldfish/goldfish.db`.

## Architecture Overview (Quick)
- Stage-first execution; pipelines orchestrate stages via queue + CAS claims.
- Async workers run in a bounded ThreadPool; recovery resumes in-flight pipelines on start.
- Observability via MCP tools: `stage_status`, `stage_logs`, `list_runs`, `get_outputs`, `get_run`, `cancel_run`.
