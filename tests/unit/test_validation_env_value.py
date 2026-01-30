import pytest

from goldfish.validation import InvalidEnvValueError, validate_env_value


def test_validate_env_value_when_goldfish_stage_config_json_allows_special_chars():
    """Goldfish internal JSON env vars should be allowed (passed via docker -e)."""
    validate_env_value("GOLDFISH_STAGE_CONFIG", '{"a": 1, "b": {"c": true}}')


def test_validate_env_value_when_contains_newline_raises():
    """Newlines can break env var argument parsing and should be rejected."""
    with pytest.raises(InvalidEnvValueError):
        validate_env_value("FOO", "line1\nline2")
