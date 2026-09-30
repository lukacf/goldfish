"""Exercise Goldfish's review flow through the installed Meerkat SDK."""

from unittest.mock import AsyncMock, patch

import pytest
from meerkat import MeerkatClient

from goldfish.svs.agent import MeerkatProvider, ReviewRequest


@pytest.mark.parametrize(
    ("response_text", "decision"),
    [("OK", "approved"), ("WARNING: Add a validation split", "warned"), ("ERROR: Data leakage", "blocked")],
)
def test_review_with_installed_sdk_parses_result_and_archives_session(monkeypatch, response_text, decision):
    """Catch SDK request/result contract changes without a binary download or LLM call."""
    monkeypatch.setenv("GOLDFISH_MEERKAT_MODEL", "claude-sonnet-4-5")
    request = ReviewRequest(review_type="pre_run", context={"prompt": "Review this training code"})
    rpc = AsyncMock(
        side_effect=[
            {
                "session_id": "review-session",
                "text": response_text,
                "turns": 1,
                "tool_calls": 0,
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
            {},
        ]
    )

    with (
        patch.object(MeerkatClient, "connect", new_callable=AsyncMock) as connect,
        patch.object(MeerkatClient, "close", new_callable=AsyncMock) as close,
        patch.object(MeerkatClient, "_request_impl", rpc),
    ):
        result = MeerkatProvider().run(request)

    assert result.decision == decision
    assert result.response_text == response_text
    assert rpc.await_count == 2
    method, params = rpc.await_args_list[0].args
    assert method == "session/create"
    assert params["prompt"] == "Review this training code"
    assert params["model"] == "claude-sonnet-4-5"
    assert "AI code reviewer for Goldfish" in params["system_prompt"]
    rpc.assert_awaited_with("session/archive", {"session_id": "review-session"})
    connect.assert_awaited_once()
    close.assert_awaited_once()
