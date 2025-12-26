"""SVS contract validation - schema as law, config resolution.

This module provides three core functions:
1. resolve_config_params() - Single authority for {param} substitution
2. merge_stage_config() - Config merge with precedence: defaults → file → runtime
3. validate_stage_contracts() - Schema validation with config resolution
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from goldfish.errors import ConfigParamNotFoundError

if TYPE_CHECKING:
    from goldfish.models import PipelineDef


def _is_wildcard_dim(value: Any) -> bool:
    """Return True if a shape dimension is a wildcard."""
    return value is None or value == -1


def _compare_shapes(
    expected_shape: list[Any] | None,
    actual_shape: list[Any] | None,
    context: str,
    errors: list[str],
) -> None:
    """Compare shapes with wildcard support."""
    if expected_shape is None:
        return
    if not isinstance(expected_shape, list):
        errors.append(f"{context}: expected shape must be a list")
        return
    if not isinstance(actual_shape, list):
        errors.append(f"{context}: metadata shape missing or invalid")
        return
    if len(expected_shape) != len(actual_shape):
        errors.append(f"{context}: shape rank mismatch ({len(expected_shape)} != {len(actual_shape)})")
        return
    for idx, (exp_dim, act_dim) in enumerate(zip(expected_shape, actual_shape, strict=False)):
        if _is_wildcard_dim(exp_dim):
            continue
        if exp_dim != act_dim:
            errors.append(f"{context}: shape[{idx}] mismatch ({exp_dim} != {act_dim})")


def _compare_dtype(expected_dtype: str | None, actual_dtype: str | None, context: str, errors: list[str]) -> None:
    if expected_dtype is None:
        return
    if not isinstance(actual_dtype, str):
        errors.append(f"{context}: metadata dtype missing or invalid")
        return
    if expected_dtype != actual_dtype:
        errors.append(f"{context}: dtype mismatch ({expected_dtype} != {actual_dtype})")


def _extract_tensor_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Return the array-level schema for tensor outputs."""
    arrays = schema.get("arrays")
    if not isinstance(arrays, dict) or not arrays:
        return schema
    primary = schema.get("primary_array")
    if isinstance(primary, str) and primary in arrays:
        result: dict[str, Any] = arrays[primary]
        return result
    # Fall back to first array definition
    first_val: dict[str, Any] = next(iter(arrays.values()))
    return first_val


def validate_output_data_against_schema(
    output_name: str,
    schema: dict[str, Any],
    data: Any,
) -> list[str]:
    """Validate output data against a schema contract.

    Supports tensor schemas (shape/dtype) and tabular schemas (columns/dtypes).
    Returns a list of error strings; empty list means valid.
    """
    errors: list[str] = []

    if not isinstance(schema, dict) or not schema:
        return errors

    kind = schema.get("kind")
    context = f"Output '{output_name}'"

    # JSON schema validation (dict or list outputs)
    if kind == "json":
        if isinstance(data, dict | list):
            return errors
        errors.append(f"{context}: expected JSON object or list")
        return errors

    # Tabular schema validation
    if kind == "tabular" or "columns" in schema or "dtypes" in schema:
        if not hasattr(data, "columns"):
            errors.append(f"{context}: expected tabular data with columns")
            return errors

        expected_columns = schema.get("columns")
        if expected_columns is not None:
            if not isinstance(expected_columns, list):
                errors.append(f"{context}: schema.columns must be a list")
            else:
                actual_columns = list(data.columns)
                if expected_columns != actual_columns:
                    errors.append(f"{context}: columns mismatch")

        expected_dtypes = schema.get("dtypes")
        if expected_dtypes is not None:
            if not isinstance(expected_dtypes, dict):
                errors.append(f"{context}: schema.dtypes must be an object")
            else:
                actual_dtypes = {}
                try:
                    actual_dtypes = {col: str(dtype) for col, dtype in data.dtypes.items()}
                except Exception:
                    errors.append(f"{context}: unable to read tabular dtypes")
                for col, expected_dtype in expected_dtypes.items():
                    actual_dtype = actual_dtypes.get(col)
                    if actual_dtype is None:
                        errors.append(f"{context}: column '{col}' dtype missing")
                    elif expected_dtype != actual_dtype:
                        errors.append(f"{context}: column '{col}' dtype mismatch ({expected_dtype} != {actual_dtype})")

        return errors

    # Tensor schema validation
    tensor_schema = schema if kind != "tensor" else _extract_tensor_schema(schema)
    expected_shape = tensor_schema.get("shape")
    expected_dtype = tensor_schema.get("dtype")

    actual_shape = None
    if hasattr(data, "shape"):
        try:
            actual_shape = list(data.shape)
        except Exception:
            actual_shape = None
    elif expected_shape is not None:
        errors.append(f"{context}: data missing shape attribute")

    actual_dtype = None
    if hasattr(data, "dtype"):
        try:
            actual_dtype = str(data.dtype)
        except Exception:
            actual_dtype = None
    elif expected_dtype is not None:
        errors.append(f"{context}: data missing dtype attribute")

    _compare_shapes(expected_shape, actual_shape, context, errors)
    _compare_dtype(expected_dtype, actual_dtype, context, errors)

    if "rank" in schema and isinstance(actual_shape, list):
        expected_rank = schema.get("rank")
        if isinstance(expected_rank, int) and len(actual_shape) != expected_rank:
            errors.append(f"{context}: rank mismatch ({len(actual_shape)} != {expected_rank})")

    return errors


def _get_config_value(stage_config: dict[str, Any], key: str) -> tuple[bool, Any]:
    """Resolve dotted config key into stage config."""
    current: Any = stage_config
    for part in key.split("."):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _validate_config_schema(stage_name: str, config_schema: dict[str, Any], stage_config: dict[str, Any]) -> list[str]:
    """Validate stage config values against declared schema."""
    errors: list[str] = []

    type_map: dict[str, tuple[type, ...]] = {
        "int": (int,),
        "float": (float, int),
        "number": (int, float),
        "bool": (bool,),
        "str": (str,),
        "list": (list,),
        "dict": (dict,),
    }

    for key, spec in config_schema.items():
        required = False
        expected_type = spec
        if isinstance(spec, dict):
            expected_type = spec.get("type")
            required = bool(spec.get("required", False))

        if not isinstance(expected_type, str) or expected_type not in type_map:
            errors.append(f"Stage '{stage_name}': config_schema '{key}' has invalid type '{expected_type}'")
            continue

        found, value = _get_config_value(stage_config, key)
        if not found:
            if required:
                errors.append(f"Stage '{stage_name}': config param '{key}' is required but missing")
            continue

        allowed_types = type_map[expected_type]
        if expected_type in ("int", "float", "number") and isinstance(value, bool):
            errors.append(f"Stage '{stage_name}': config param '{key}' must be {expected_type}, got bool")
            continue

        if not isinstance(value, allowed_types):
            errors.append(
                f"Stage '{stage_name}': config param '{key}' must be {expected_type}, got {type(value).__name__}"
            )

    return errors


def _validate_schema_types(schema: dict[str, Any], output_name: str, errors: list[str]) -> None:
    """Validate schema value types (shape dims, rank)."""
    if "rank" in schema:
        rank = schema.get("rank")
        if not isinstance(rank, int):
            errors.append(f"Output '{output_name}': rank must be int, got {type(rank).__name__}")
        elif rank < 0:
            errors.append(f"Output '{output_name}': rank must be >= 0")

    if "shape" in schema:
        shape = schema.get("shape")
        if not isinstance(shape, list):
            errors.append(f"Output '{output_name}': shape must be a list")
            return
        for idx, dim in enumerate(shape):
            if _is_wildcard_dim(dim):
                continue
            if not isinstance(dim, int):
                errors.append(f"Output '{output_name}': shape[{idx}] must be int or null, got {type(dim).__name__}")
            elif dim < 0:
                errors.append(f"Output '{output_name}': shape[{idx}] must be >= 0 or -1")


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
                # Handle both list format [{name: x, ...}] and dict format {x: {...}}
                if isinstance(stages, list):
                    for stage_def in stages:
                        if stage_def.get("name") == stage_name:
                            if "defaults" in stage_def:
                                defaults = stage_def["defaults"]
                                if defaults:
                                    result.update(defaults)
                            break
                elif isinstance(stages, dict) and stage_name in stages:
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

        _validate_schema_types(resolved_schema, output_name, errors)

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


def validate_cross_stage_shapes(pipeline: PipelineDef, workspace_path: Path) -> list[str]:
    """Validate schema compatibility across connected stages.

    Resolves schemas with per-stage configs, then compares shapes and dtypes
    for connected signals.

    Args:
        pipeline: Parsed pipeline definition
        workspace_path: Workspace path for config resolution

    Returns:
        List of error strings (empty list = valid)
    """
    errors: list[str] = []

    # Resolve output schemas per stage
    output_schemas: dict[tuple[str, str], dict[str, Any]] = {}
    for stage in pipeline.stages:
        stage_config = merge_stage_config(stage.name, workspace_path)
        for signal_name, output_def in stage.outputs.items():
            schema = output_def.output_schema
            if not schema:
                continue
            try:
                resolved_schema = resolve_config_params(schema, stage_config)
            except ConfigParamNotFoundError as e:
                param = e.details["param"]
                errors.append(f"Stage '{stage.name}' output '{signal_name}': missing config param '{param}'")
                continue
            _validate_schema_types(resolved_schema, f"{stage.name}.{signal_name}", errors)
            output_schemas[(stage.name, signal_name)] = resolved_schema

    # Compare input schemas to upstream output schemas
    for stage in pipeline.stages:
        stage_config = merge_stage_config(stage.name, workspace_path)
        for input_name, input_def in stage.inputs.items():
            if not input_def.from_stage:
                continue
            if not input_def.output_schema:
                continue

            source_name = input_def.signal or input_name
            source_key = (input_def.from_stage, source_name)
            source_schema = output_schemas.get(source_key)
            if source_schema is None:
                errors.append(
                    f"Stage '{stage.name}' input '{input_name}': "
                    f"source schema missing for '{input_def.from_stage}.{source_name}'"
                )
                continue

            try:
                expected_schema = resolve_config_params(input_def.output_schema, stage_config)
            except ConfigParamNotFoundError as e:
                param = e.details["param"]
                errors.append(f"Stage '{stage.name}' input '{input_name}': missing config param '{param}'")
                continue

            _validate_schema_types(expected_schema, f"{stage.name}.{input_name}", errors)

            source_kind = source_schema.get("kind")
            expected_kind = expected_schema.get("kind")
            if source_kind and expected_kind and source_kind != expected_kind:
                errors.append(
                    f"Stage '{stage.name}' input '{input_name}': schema.kind mismatch "
                    f"({source_kind} != {expected_kind})"
                )
                continue

            # Compare dtype if both defined
            source_dtype = source_schema.get("dtype")
            expected_dtype = expected_schema.get("dtype")
            if source_dtype is not None and expected_dtype is not None and source_dtype != expected_dtype:
                errors.append(
                    f"Stage '{stage.name}' input '{input_name}': dtype mismatch "
                    f"({source_dtype} != {expected_dtype})"
                )

            # Compare shapes (allow wildcards)
            source_shape = source_schema.get("shape")
            expected_shape = expected_schema.get("shape")
            if isinstance(source_shape, list) and isinstance(expected_shape, list):
                if len(source_shape) != len(expected_shape):
                    errors.append(
                        f"Stage '{stage.name}' input '{input_name}': shape rank mismatch "
                        f"({len(source_shape)} != {len(expected_shape)})"
                    )
                    continue
                for idx, (src_dim, exp_dim) in enumerate(zip(source_shape, expected_shape, strict=False)):
                    if _is_wildcard_dim(src_dim) or _is_wildcard_dim(exp_dim):
                        continue
                    if src_dim != exp_dim:
                        errors.append(
                            f"Stage '{stage.name}' input '{input_name}': shape[{idx}] mismatch "
                            f"({src_dim} != {exp_dim})"
                        )

    return errors


def validate_input_schema_against_metadata(
    input_name: str,
    input_schema: dict[str, Any],
    metadata: dict[str, Any],
) -> list[str]:
    """Validate an input schema contract against registered metadata."""
    errors: list[str] = []

    if not isinstance(metadata, dict):
        return [f"Input '{input_name}': metadata missing or invalid"]

    meta_schema = metadata.get("schema")
    if not isinstance(meta_schema, dict):
        return [f"Input '{input_name}': metadata schema missing"]

    expected_kind = input_schema.get("kind")
    actual_kind = meta_schema.get("kind")
    if expected_kind and expected_kind != actual_kind:
        errors.append(f"Input '{input_name}': schema.kind mismatch ({expected_kind} != {actual_kind})")

    if actual_kind == "tensor":
        meta_arrays = meta_schema.get("arrays")
        if not isinstance(meta_arrays, dict):
            errors.append(f"Input '{input_name}': metadata arrays missing for tensor schema")
            return errors

        expected_arrays = input_schema.get("arrays")
        if isinstance(expected_arrays, dict):
            for array_name, expected_array in expected_arrays.items():
                if array_name not in meta_arrays:
                    errors.append(f"Input '{input_name}': missing array '{array_name}' in metadata")
                    continue
                if not isinstance(expected_array, dict):
                    errors.append(f"Input '{input_name}': expected array '{array_name}' must be an object")
                    continue
                actual_array = meta_arrays.get(array_name, {})
                context = f"Input '{input_name}' array '{array_name}'"
                _compare_shapes(expected_array.get("shape"), actual_array.get("shape"), context, errors)
                _compare_dtype(expected_array.get("dtype"), actual_array.get("dtype"), context, errors)
            return errors

        # Fallback: compare against primary array using top-level shape/dtype
        primary = meta_schema.get("primary_array")
        if isinstance(primary, str) and primary in meta_arrays:
            actual_array = meta_arrays[primary]
            context = f"Input '{input_name}' primary array '{primary}'"
            _compare_shapes(input_schema.get("shape"), actual_array.get("shape"), context, errors)
            _compare_dtype(input_schema.get("dtype"), actual_array.get("dtype"), context, errors)
        else:
            errors.append(f"Input '{input_name}': metadata primary_array missing for tensor schema")

    elif actual_kind == "tabular":
        expected_columns = input_schema.get("columns")
        if expected_columns is not None:
            meta_columns = meta_schema.get("columns")
            if not isinstance(expected_columns, list):
                errors.append(f"Input '{input_name}': expected columns must be a list")
            elif not isinstance(meta_columns, list):
                errors.append(f"Input '{input_name}': metadata columns missing for tabular schema")
            else:
                if expected_columns != meta_columns:
                    errors.append(f"Input '{input_name}': columns mismatch")
        expected_dtypes = input_schema.get("dtypes")
        if expected_dtypes is not None:
            meta_dtypes = meta_schema.get("dtypes")
            if not isinstance(expected_dtypes, dict):
                errors.append(f"Input '{input_name}': expected dtypes must be an object")
            elif not isinstance(meta_dtypes, dict):
                errors.append(f"Input '{input_name}': metadata dtypes missing for tabular schema")
            else:
                for col, dtype in expected_dtypes.items():
                    actual_dtype = meta_dtypes.get(col)
                    if actual_dtype is None:
                        errors.append(f"Input '{input_name}': column '{col}' missing in metadata dtypes")
                    elif dtype != actual_dtype:
                        errors.append(
                            f"Input '{input_name}': column '{col}' dtype mismatch ({dtype} != {actual_dtype})"
                        )

    return errors


def validate_stage_config_schema(
    stage_name: str, config_schema: dict[str, Any] | None, stage_config: dict[str, Any]
) -> list[str]:
    """Public entry point for config schema validation."""
    if not config_schema:
        return []
    return _validate_config_schema(stage_name, config_schema, stage_config)
