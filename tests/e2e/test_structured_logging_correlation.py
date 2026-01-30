"""E2E scenario tests for structured logging correlation IDs.

E2E-003 (spec): Structured logging with trace IDs.
"""

from __future__ import annotations

import importlib.util
import json
import logging
from io import StringIO


def test_structured_logging_when_stage_execution_logs_emitted_contains_stage_run_id() -> None:
    """E2E-003: Stage execution logs are JSON and include stage_run_id."""
    logging_spec = importlib.util.find_spec("goldfish.config.logging")
    assert logging_spec is not None, "Expected module 'goldfish.config.logging' to exist in Phase 7."

    from goldfish.config.logging import StructuredFormatter, current_stage_run_id

    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredFormatter())

    logger = logging.getLogger("goldfish.e2e.logging")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)

    token = current_stage_run_id.set("stage-abc123")
    try:
        logger.info("hello world")
    finally:
        current_stage_run_id.reset(token)

    handler.flush()
    line = stream.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["stage_run_id"] == "stage-abc123"
