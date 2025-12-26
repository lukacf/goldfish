"""Unit tests for SVS librarian review logic."""

from __future__ import annotations

import json

from goldfish.server_tools import svs_tools
from goldfish.svs.agent import ReviewRequest, ReviewResult
from goldfish.svs.config import SVSConfig


class _StubAgent:
    name = "stub"

    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.last_request: ReviewRequest | None = None

    def run(self, request: ReviewRequest) -> ReviewResult:
        self.last_request = request
        return ReviewResult(
            decision="approved",
            findings=[],
            response_text=self.response_text,
            duration_ms=5,
        )


def test_librarian_review_parses_json_response():
    """Librarian should parse JSON recommendations from agent output."""
    patterns = [
        {"id": "pat-001", "symptom": "OOM"},
        {"id": "pat-002", "symptom": "KeyError"},
    ]
    response = json.dumps(
        {
            "pat-001": {"action": "approve", "confidence": "high", "reason": "Valid"},
            "pat-002": {"action": "reject", "confidence": "low", "reason": "False positive"},
        }
    )
    agent = _StubAgent(response)
    config = SVSConfig(enabled=True, agent_provider="null")

    recommendations = svs_tools.librarian_review_patterns(patterns, agent=agent, config=config)

    assert recommendations["pat-001"]["action"] == "approve"
    assert recommendations["pat-002"]["action"] == "reject"
    assert agent.last_request is not None
    assert "prompt" in agent.last_request.context
