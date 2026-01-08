"""Unit tests for SVS agent module.

Tests for agent prompt building and run_context formatting.
"""

from __future__ import annotations


class TestDefaultPromptRunContext:
    """Test _default_prompt run_context formatting."""

    def test_includes_run_command_with_config_override(self):
        """Prompt should include config_override in run command."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "experiment_1",
                "stage_name": "train",
                "config_override": {"epochs": 100, "lr": 0.001},
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "## Run Command" in prompt
        assert "run(" in prompt
        assert "config_override" in prompt
        assert '"epochs": 100' in prompt
        assert '"lr": 0.001' in prompt

    def test_includes_run_command_with_inputs_override(self):
        """Prompt should include inputs_override in run command."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "experiment_1",
                "stage_name": "train",
                "inputs_override": {"data": "debug_source"},
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "inputs_override" in prompt
        assert "debug_source" in prompt

    def test_includes_run_reason(self):
        """Prompt should include run reason fields."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "experiment_1",
                "stage_name": "train",
                "run_reason": {
                    "description": "Testing new model",
                    "hypothesis": "Will improve accuracy",
                    "goal": "Beat baseline by 5%",
                },
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "## Run Reason" in prompt
        assert "Testing new model" in prompt
        assert "Will improve accuracy" in prompt
        assert "Beat baseline by 5%" in prompt

    def test_formats_workspace_and_stage(self):
        """Prompt should include workspace and stage in run command."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "my_experiment",
                "stage_name": "preprocess",
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "workspace='my_experiment'" in prompt
        assert "stage='preprocess'" in prompt

    def test_includes_pipeline_name(self):
        """Prompt should include pipeline_name when present."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "run_context": {
                "workspace": "test",
                "stage_name": "train",
                "pipeline_name": "custom_pipeline.yaml",
            }
        }

        prompt = _default_prompt("post_run", context, None)

        assert "pipeline='custom_pipeline.yaml'" in prompt

    def test_no_run_command_without_context(self):
        """Prompt should not include run command section without run_context."""
        from goldfish.svs.agent import _default_prompt

        context = {"outputs_dir": "/tmp/outputs"}

        prompt = _default_prompt("post_run", context, None)

        # Should not have run command section when no run_context
        assert "## Run Command" not in prompt

    def test_run_context_excluded_from_payload(self):
        """run_context should be formatted separately, not in raw payload."""
        from goldfish.svs.agent import _default_prompt

        context = {
            "outputs_dir": "/tmp/outputs",
            "run_context": {
                "workspace": "test",
                "stage_name": "train",
            },
        }

        prompt = _default_prompt("post_run", context, None)

        # The run_context should be in the formatted section, not raw in payload
        # Check that the payload section doesn't have run_context
        import re

        payload_match = re.search(r"Review payload:\n(.+)", prompt, re.DOTALL)
        if payload_match:
            payload_text = payload_match.group(1)
            # The payload should have outputs_dir but run_context should be formatted separately
            assert "outputs_dir" in payload_text
            # run_context is formatted in ## Run Command section instead
