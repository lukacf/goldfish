"""Structured logging utilities for the architecture redesign."""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar
from datetime import UTC, datetime

current_stage_run_id: ContextVar[str | None] = ContextVar("goldfish_stage_run_id", default=None)
current_request_id: ContextVar[str | None] = ContextVar("goldfish_request_id", default=None)


class StructuredFormatter(logging.Formatter):
    """Format log records as one-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        stage_run_id = current_stage_run_id.get()
        if stage_run_id is not None:
            payload["stage_run_id"] = stage_run_id

        request_id = current_request_id.get()
        if request_id is not None:
            payload["request_id"] = request_id

        return json.dumps(payload, ensure_ascii=False)
