"""Unit tests for structured logging utilities.

These tests define the expected behavior for `goldfish.config.logging`, which
provides JSON formatted log output with correlation context.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from io import StringIO


def _emit_log(formatter: logging.Formatter, message: str, *args: object) -> dict[str, object]:
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter)

    test_logger = logging.getLogger("goldfish.unit.logging")
    prev_handlers = list(test_logger.handlers)
    prev_level = test_logger.level
    prev_propagate = test_logger.propagate

    test_logger.handlers = [handler]
    test_logger.setLevel(logging.INFO)
    test_logger.propagate = False
    try:
        test_logger.info(message, *args)
        handler.flush()
    finally:
        test_logger.handlers = prev_handlers
        test_logger.setLevel(prev_level)
        test_logger.propagate = prev_propagate

    line = stream.getvalue().strip().splitlines()[-1]
    return json.loads(line)


def test_structured_formatter_when_info_logged_outputs_json_fields() -> None:
    """StructuredFormatter emits JSON with basic log fields."""
    from goldfish.config.logging import StructuredFormatter

    payload = _emit_log(StructuredFormatter(), "hello world")

    assert isinstance(payload["timestamp"], str)
    datetime.fromisoformat(payload["timestamp"])
    assert payload["level"] == "INFO"
    assert payload["logger"] == "goldfish.unit.logging"
    assert payload["message"] == "hello world"


def test_structured_formatter_when_format_args_used_formats_message() -> None:
    """StructuredFormatter uses `LogRecord.getMessage()` formatting."""
    from goldfish.config.logging import StructuredFormatter

    payload = _emit_log(StructuredFormatter(), "hello %s", "world")

    assert payload["message"] == "hello world"


def test_context_var_propagation_when_not_set_omits_correlation_fields() -> None:
    """StructuredFormatter omits correlation IDs when they are not available."""
    from goldfish.config.logging import StructuredFormatter

    payload = _emit_log(StructuredFormatter(), "hello world")

    assert "stage_run_id" not in payload
    assert "request_id" not in payload


def test_context_var_propagation_when_stage_run_id_set_includes_field() -> None:
    """StructuredFormatter includes stage_run_id when current_stage_run_id is set."""
    from goldfish.config.logging import StructuredFormatter, current_stage_run_id

    token = current_stage_run_id.set("stage-abc123")
    try:
        payload = _emit_log(StructuredFormatter(), "hello world")
    finally:
        current_stage_run_id.reset(token)

    assert payload["stage_run_id"] == "stage-abc123"


def test_context_var_propagation_when_request_id_set_includes_field() -> None:
    """StructuredFormatter includes request_id when current_request_id is set."""
    from goldfish.config.logging import StructuredFormatter, current_request_id

    token = current_request_id.set("req-xyz789")
    try:
        payload = _emit_log(StructuredFormatter(), "hello world")
    finally:
        current_request_id.reset(token)

    assert payload["request_id"] == "req-xyz789"
