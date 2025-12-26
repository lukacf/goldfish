"""Unit tests for SVSConfig - Semantic Validation System configuration."""

import pytest
from pydantic import ValidationError


class TestSVSConfigDefaults:
    """Tests for SVSConfig default values."""

    def test_default_enabled(self):
        """SVS should be enabled by default for full implementation."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.enabled is True

    def test_default_domain(self):
        """Default domain should be 'default'."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.domain == "default"

    def test_default_policy_is_warn(self):
        """Default policy should favor safety (warn, not fail)."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.default_policy == "warn"
        assert config.default_enforcement == "warning"

    def test_default_stats_enabled(self):
        """Stats should be enabled by default."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.stats_enabled is True

    def test_default_ai_pre_run_enabled(self):
        """AI pre-run review should be enabled by default."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.ai_pre_run_enabled is True

    def test_default_ai_post_run_enabled(self):
        """AI post-run review should be enabled by default (full implementation)."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.ai_post_run_enabled is True

    def test_default_agent_provider(self):
        """Default agent provider should be claude_code."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.agent_provider == "claude_code"

    def test_default_agent_timeout(self):
        """Default agent timeout should be 120 seconds."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.agent_timeout == 120

    def test_default_agent_max_turns(self):
        """Default max turns should be 3."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.agent_max_turns == 3

    def test_default_rate_limit(self):
        """Default rate limit should be 60 per hour."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.rate_limit_per_hour == 60

    def test_default_auto_learn_failures(self):
        """Auto-learn failures should be enabled by default (full implementation)."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        assert config.auto_learn_failures is True


class TestSVSConfigValidation:
    """Tests for SVSConfig field validation."""

    def test_rejects_invalid_domain(self):
        """Should reject invalid domain values."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError):
            SVSConfig(domain="")

    def test_accepts_valid_domains(self):
        """Should accept valid domain values."""
        from goldfish.svs.config import SVSConfig

        valid_domains = ["default", "nlp_tokenizer", "image_embeddings", "tabular_features"]
        for domain in valid_domains:
            config = SVSConfig(domain=domain)
            assert config.domain == domain

    def test_rejects_invalid_policy(self):
        """Should reject invalid policy values."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError):
            SVSConfig(default_policy="invalid")

    def test_accepts_valid_policies(self):
        """Should accept valid policy values."""
        from goldfish.svs.config import SVSConfig

        for policy in ["fail", "warn", "ignore"]:
            config = SVSConfig(default_policy=policy)
            assert config.default_policy == policy

    def test_rejects_invalid_enforcement(self):
        """Should reject invalid enforcement values."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError):
            SVSConfig(default_enforcement="invalid")

    def test_accepts_valid_enforcement(self):
        """Should accept valid enforcement values."""
        from goldfish.svs.config import SVSConfig

        for enforcement in ["blocking", "warning", "silent"]:
            config = SVSConfig(default_enforcement=enforcement)
            assert config.default_enforcement == enforcement

    def test_rejects_negative_timeout(self):
        """Should reject negative timeout values."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError):
            SVSConfig(agent_timeout=-1)

    def test_rejects_zero_max_turns(self):
        """Should reject zero max turns."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError):
            SVSConfig(agent_max_turns=0)

    def test_rejects_negative_rate_limit(self):
        """Should reject negative rate limit."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError):
            SVSConfig(rate_limit_per_hour=-1)

    def test_accepts_zero_rate_limit(self):
        """Should accept zero rate limit (disabled)."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig(rate_limit_per_hour=0)
        assert config.rate_limit_per_hour == 0

    def test_rejects_invalid_agent_provider(self):
        """Should reject unknown agent provider."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError):
            SVSConfig(agent_provider="unknown_provider")

    def test_accepts_valid_agent_providers(self):
        """Should accept valid agent providers."""
        from goldfish.svs.config import SVSConfig

        for provider in ["claude_code", "null"]:
            config = SVSConfig(agent_provider=provider)
            assert config.agent_provider == provider


class TestSVSConfigExtraForbidden:
    """Tests that unknown fields are rejected."""

    def test_rejects_extra_fields(self):
        """Should reject unknown configuration fields."""
        from goldfish.svs.config import SVSConfig

        with pytest.raises(ValidationError) as exc_info:
            SVSConfig(unknown_field="value")

        # Verify error mentions the unknown field
        assert "unknown_field" in str(exc_info.value).lower() or "extra" in str(exc_info.value).lower()


class TestSVSConfigSerialization:
    """Tests for config serialization/deserialization."""

    def test_roundtrip_defaults(self):
        """Default config should survive serialization roundtrip."""
        from goldfish.svs.config import SVSConfig

        original = SVSConfig()
        data = original.model_dump()
        restored = SVSConfig(**data)

        assert restored.enabled == original.enabled
        assert restored.domain == original.domain
        assert restored.default_policy == original.default_policy
        assert restored.agent_provider == original.agent_provider

    def test_roundtrip_custom(self):
        """Custom config should survive serialization roundtrip."""
        from goldfish.svs.config import SVSConfig

        original = SVSConfig(
            enabled=False,
            domain="nlp_tokenizer",
            default_policy="fail",
            agent_timeout=60,
            auto_learn_failures=False,
        )
        data = original.model_dump()
        restored = SVSConfig(**data)

        assert restored.enabled == original.enabled
        assert restored.domain == original.domain
        assert restored.default_policy == original.default_policy
        assert restored.agent_timeout == original.agent_timeout
        assert restored.auto_learn_failures == original.auto_learn_failures

    def test_exclude_none_serialization(self):
        """Serialization should handle None values correctly."""
        from goldfish.svs.config import SVSConfig

        config = SVSConfig()
        data = config.model_dump(exclude_none=True)

        # All required fields should be present
        assert "enabled" in data
        assert "domain" in data
