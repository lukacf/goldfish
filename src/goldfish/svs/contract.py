"""SVS contract validation - schema as law, config resolution.

This module provides three core functions:
1. resolve_config_params() - Single authority for {param} substitution
2. merge_stage_config() - Config merge with precedence: defaults → file → runtime
3. validate_stage_contracts() - Schema validation with config resolution
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from goldfish.errors import ConfigParamNotFoundError


def resolve_config_params(value: Any, stage_config: dict[str, Any]) -> Any:
    """Resolve {param} placeholders in values using stage config.

    Single authority for parameter substitution. Handles nested dicts and lists
    recursively.

    Args:
        value: Value to resolve (can be string, dict, list, or primitive)
        stage_config: Config dict to resolve parameters from

    Returns:
        Resolved value with all {param} placeholders substituted

    Raises:
        ConfigParamNotFoundError: If a referenced parameter is not in stage_config
    """
    # Handle strings with {param} syntax
    if isinstance(value, str):
        # Check if it's a parameter reference
        if value.startswith("{") and value.endswith("}"):
            param_name = value[1:-1]
            if param_name not in stage_config:
                raise ConfigParamNotFoundError(param=param_name, available=list(stage_config.keys()))
            return stage_config[param_name]
        return value

    # Handle dicts recursively
    if isinstance(value, dict):
        return {k: resolve_config_params(v, stage_config) for k, v in value.items()}

    # Handle lists recursively
    if isinstance(value, list):
        return [resolve_config_params(item, stage_config) for item in value]

    # Pass through primitives (int, float, bool, None)
    return value


def merge_stage_config(
    stage_name: str,
    workspace_path: Path,
    runtime_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge stage config with explicit precedence: defaults → file → runtime.

    Args:
        stage_name: Name of the stage to load config for
        workspace_path: Path to workspace containing pipeline.yaml and configs/
        runtime_overrides: Optional runtime config overrides (highest precedence)

    Returns:
        Merged config dict
    """
    result: dict[str, Any] = {}

    # 1. Load defaults from pipeline.yaml
    pipeline_path = workspace_path / "pipeline.yaml"
    if pipeline_path.exists():
        with open(pipeline_path) as f:
            pipeline_data = yaml.safe_load(f)
            if pipeline_data and "stages" in pipeline_data:
                stages = pipeline_data["stages"]
                if stage_name in stages:
                    stage_def = stages[stage_name]
                    if "defaults" in stage_def:
                        defaults = stage_def["defaults"]
                        if defaults:
                            result.update(defaults)

    # 2. Load and merge configs/{stage_name}.yaml
    config_path = workspace_path / "configs" / f"{stage_name}.yaml"
    if config_path.exists():
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
            if config_data:
                result.update(config_data)

    # 3. Apply runtime overrides (highest precedence)
    if runtime_overrides:
        result.update(runtime_overrides)

    return result


def validate_stage_contracts(stage_def: dict[str, Any], stage_config: dict[str, Any]) -> list[str]:
    """Validate stage output schemas with config resolution.

    Validates:
    - Shape/rank consistency: len(shape) must equal rank if both specified
    - Resolves {param} placeholders in schemas before validation

    Args:
        stage_def: Stage definition dict (from pipeline.yaml)
        stage_config: Merged stage config for parameter resolution

    Returns:
        List of error strings (empty list = valid)
    """
    errors: list[str] = []

    # No outputs = valid
    if "outputs" not in stage_def:
        return errors

    outputs = stage_def["outputs"]
    if not outputs:
        return errors

    # Validate each output
    for output_name, output_def in outputs.items():
        # No schema = skip validation
        if "schema" not in output_def:
            continue

        schema = output_def["schema"]
        if not schema:
            continue

        # Try to resolve params in schema
        try:
            resolved_schema = resolve_config_params(schema, stage_config)
        except ConfigParamNotFoundError as e:
            # Missing param in schema
            param = e.details["param"]
            errors.append(f"Output '{output_name}': schema references missing config param '{param}'")
            continue

        # Validate shape/rank consistency
        if "shape" in resolved_schema and "rank" in resolved_schema:
            shape = resolved_schema["shape"]
            rank = resolved_schema["rank"]

            if isinstance(shape, list) and isinstance(rank, int):
                actual_rank = len(shape)
                if actual_rank != rank:
                    errors.append(
                        f"Output '{output_name}': shape/rank mismatch - "
                        f"shape has {actual_rank} dimensions but rank is {rank}"
                    )

    return errors
