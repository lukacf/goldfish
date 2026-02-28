"""Unit tests for SVS MeerkatProvider.

Tests the Meerkat SDK-based agent provider for SVS code reviews.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

from goldfish.svs.agent import ReviewRequest


def _make_mock_client(mock_session):
    """Create a mock MeerkatClient that supports async context manager."""
    mock_client = MagicMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    async def mock_create(**kwargs):
        return mock_session

    mock_client.create_session = mock_create
    return mock_client


def _make_mock_module(mock_client):
    """Create a mock meerkat module whose MeerkatClient() returns mock_client."""
    mock_mod = MagicMock()
    mock_mod.MeerkatClient = lambda: mock_client
    return mock_mod


class TestMeerkatProviderBasics:
    """Basic MeerkatProvider tests."""

    def test_meerkat_provider_has_correct_name(self):
        """Provider name should be 'meerkat'."""
        from goldfish.svs.agent import MeerkatProvider

        provider = MeerkatProvider()
        assert provider.name == "meerkat"


class TestMeerkatProviderRun:
    """Tests for MeerkatProvider.run()."""

    def test_meerkat_provider_run_returns_review_result(self):
        """run() should return ReviewResult with parsed findings from Meerkat session."""
        from goldfish.svs.agent import MeerkatProvider, ReviewResult

        provider = MeerkatProvider()
        request = ReviewRequest(
            review_type="pre_run",
            context={"prompt": "Review this code"},
        )

        mock_session = MagicMock()
        mock_session.text = "NOTE: Code looks clean\nWARNING: Consider adding tests"
        mock_session.id = "test-session-456"
        mock_session.archive = AsyncMock()

        mock_client = _make_mock_client(mock_session)

        with patch.dict(os.environ, {}, clear=False):
            with patch("goldfish.svs.agent._import_meerkat") as mock_import:
                mock_import.return_value = _make_mock_module(mock_client)
                result = provider.run(request)

        assert isinstance(result, ReviewResult)
        assert result.decision == "warned"
        assert any("WARNING" in f for f in result.findings)
        assert any("NOTE" in f for f in result.findings)

    def test_meerkat_provider_fails_open_on_sdk_import_error(self):
        """Should return approved when meerkat SDK is not importable."""
        from goldfish.svs.agent import MeerkatProvider

        provider = MeerkatProvider()
        request = ReviewRequest(review_type="pre_run", context={})

        with patch("goldfish.svs.agent._import_meerkat", side_effect=ImportError("No module named 'meerkat'")):
            result = provider.run(request)

        assert result.decision == "approved"
        assert any("WARNING" in f for f in result.findings)

    def test_meerkat_provider_fails_open_on_runtime_error(self):
        """Should return approved when Meerkat SDK raises at runtime."""
        from goldfish.svs.agent import MeerkatProvider

        provider = MeerkatProvider()
        request = ReviewRequest(review_type="pre_run", context={})

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        async def exploding_create(**kwargs):
            raise RuntimeError("Meerkat RPC failed")

        mock_client.create_session = exploding_create

        with patch("goldfish.svs.agent._import_meerkat") as mock_import:
            mock_import.return_value = _make_mock_module(mock_client)
            result = provider.run(request)

        assert result.decision == "approved"
        assert any("WARNING" in f or "failed" in f.lower() for f in result.findings)

    def test_meerkat_provider_passes_model_from_env_var(self):
        """Should use GOLDFISH_MEERKAT_MODEL env var when set."""
        from goldfish.svs.agent import MeerkatProvider

        provider = MeerkatProvider()
        request = ReviewRequest(review_type="pre_run", context={"prompt": "test"})

        mock_session = MagicMock()
        mock_session.text = "OK"
        mock_session.id = "test-session"
        mock_session.archive = AsyncMock()

        captured_kwargs: dict = {}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        async def capture_create(**kwargs):
            captured_kwargs.update(kwargs)
            return mock_session

        mock_client.create_session = capture_create

        with patch.dict(os.environ, {"GOLDFISH_MEERKAT_MODEL": "claude-sonnet-4-5-20250514"}, clear=False):
            with patch("goldfish.svs.agent._import_meerkat") as mock_import:
                mock_import.return_value = _make_mock_module(mock_client)
                provider.run(request)

        assert captured_kwargs.get("model") == "claude-sonnet-4-5-20250514"

    def test_meerkat_provider_uses_default_model_when_no_env(self):
        """Should not pass model kwarg when GOLDFISH_MEERKAT_MODEL is not set."""
        from goldfish.svs.agent import MeerkatProvider

        provider = MeerkatProvider()
        request = ReviewRequest(review_type="pre_run", context={"prompt": "test"})

        mock_session = MagicMock()
        mock_session.text = "OK"
        mock_session.id = "test-session"
        mock_session.archive = AsyncMock()

        captured_kwargs: dict = {}

        mock_client = MagicMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        async def capture_create(**kwargs):
            captured_kwargs.update(kwargs)
            return mock_session

        mock_client.create_session = capture_create

        env = os.environ.copy()
        env.pop("GOLDFISH_MEERKAT_MODEL", None)

        with patch.dict(os.environ, env, clear=True):
            with patch("goldfish.svs.agent._import_meerkat") as mock_import:
                mock_import.return_value = _make_mock_module(mock_client)
                provider.run(request)

        assert "model" not in captured_kwargs

    def test_meerkat_provider_archives_session_after_use(self):
        """Should call session.archive() for cleanup after review."""
        from goldfish.svs.agent import MeerkatProvider

        provider = MeerkatProvider()
        request = ReviewRequest(review_type="pre_run", context={"prompt": "test"})

        archive_called = []

        async def mock_archive():
            archive_called.append(True)

        mock_session = MagicMock()
        mock_session.text = "OK"
        mock_session.id = "sess-123"
        mock_session.archive = mock_archive

        mock_client = _make_mock_client(mock_session)

        with patch("goldfish.svs.agent._import_meerkat") as mock_import:
            mock_import.return_value = _make_mock_module(mock_client)
            provider.run(request)

        assert archive_called == [True]


class TestGetAgentProviderMeerkat:
    """Tests for get_agent_provider() with meerkat."""

    def test_get_agent_provider_returns_meerkat_when_sdk_available(self):
        """Should return MeerkatProvider when SDK is importable."""
        from goldfish.svs.agent import MeerkatProvider, get_agent_provider

        with patch("goldfish.svs.agent.importlib.util.find_spec") as mock_find:
            mock_find.return_value = MagicMock()  # non-None = found
            provider = get_agent_provider("meerkat")

        assert isinstance(provider, MeerkatProvider)

    def test_get_agent_provider_meerkat_falls_back_when_sdk_missing(self):
        """Should fall back to NullProvider when meerkat SDK is not installed."""
        from goldfish.svs.agent import NullProvider, get_agent_provider

        with patch("goldfish.svs.agent.importlib.util.find_spec") as mock_find:
            mock_find.return_value = None  # SDK not found
            provider = get_agent_provider("meerkat")

        assert isinstance(provider, NullProvider)
