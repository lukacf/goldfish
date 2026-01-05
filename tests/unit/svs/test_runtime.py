"""Unit tests for SVS runtime helpers."""

import os
from unittest.mock import patch

from goldfish.svs.runtime import get_stop_reason, should_stop


def test_should_stop_detects_file(tmp_path):
    outputs_dir = tmp_path / "outputs"
    goldfish_dir = outputs_dir / ".goldfish"
    goldfish_dir.mkdir(parents=True)
    stop_file = goldfish_dir / "stop_requested"

    with patch.dict(os.environ, {"GOLDFISH_OUTPUTS_DIR": str(outputs_dir)}):
        assert should_stop() is False

        stop_file.write_text("Test reason")
        assert should_stop() is True
        assert get_stop_reason() == "Test reason"


def test_get_stop_reason_missing_file(tmp_path):
    outputs_dir = tmp_path / "outputs"
    with patch.dict(os.environ, {"GOLDFISH_OUTPUTS_DIR": str(outputs_dir)}):
        assert get_stop_reason() is None
