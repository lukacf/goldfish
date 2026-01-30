"""Pipeline validation for dry-run mode.

Validates pipeline configuration without launching.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Optional

import yaml

from goldfish.db.database import Database
from goldfish.errors import ConfigParamNotFoundError
from goldfish.pipeline.parser import PipelineNotFoundError, PipelineParser
from goldfish.svs.contract import (
    merge_stage_config,
    resolve_config_params,
    validate_input_schema_against_metadata,
    validate_stage_contracts,
)
from goldfish.validation import parse_source_metadata

if TYPE_CHECKING:
    from goldfish.config import GoldfishConfig


def validate_pipeline_run(
    workspace_name: str,
    workspace_path: Path,
    db: Database,
    stages: list[str] | None,
    pipeline_name: str | None,
    inputs_override: dict,
    config: Optional["GoldfishConfig"] = None,
    config_override: dict | None = None,
) -> dict:
    """Validate a pipeline run without actually launching.

    Args:
        workspace_name: Name of the workspace
        workspace_path: Path to workspace files
        db: Database connection for checking datasets
        stages: Specific stages to run (or None for all)
        pipeline_name: Pipeline file name (or None for default)
        inputs_override: Input overrides that skip validation
        config: Full project configuration (optional)

    Returns:
        dict with:
        - valid: bool
        - stages_to_run: list of stage names
        - validation_errors: list of error messages
        - warnings: list of warning messages
    """
    errors: list[str] = []
    warnings: list[str] = []
    stages_to_run: list[str] = []

    # 1. Check pipeline exists and parse it
    try:
        parser = PipelineParser()
        # Determine pipeline file path
        if pipeline_name:
            pipeline_path = workspace_path / "pipelines" / f"{pipeline_name}.yaml"
            if not pipeline_path.exists():
                pipeline_path = workspace_path / f"{pipeline_name}.yaml"
        else:
            pipeline_path = workspace_path / "pipeline.yaml"

        pipeline_def = parser.parse(pipeline_path)
        all_stages = [s.name for s in pipeline_def.stages]

        # Run full pipeline validation including cross-stage schema checks
        validation_errors = parser.validate(pipeline_def, workspace_path)
        errors.extend(validation_errors)

        # Determine which stages to run
        if stages:
            for s in stages:
                if s not in all_stages:
                    errors.append(f"Stage '{s}' not found in pipeline. Available: {', '.join(all_stages)}")
                else:
                    stages_to_run.append(s)
        else:
            stages_to_run = all_stages

    except PipelineNotFoundError as e:
        errors.append(str(e))
        return {
            "valid": False,
            "stages_to_run": [],
            "validation_errors": errors,
            "warnings": warnings,
        }
    except Exception as e:
        errors.append(f"Pipeline parse error: {e}")
        return {
            "valid": False,
            "stages_to_run": [],
            "validation_errors": errors,
            "warnings": warnings,
        }

    # 2. Validate each stage
    for stage_name in stages_to_run:
        stage_def = next((s for s in pipeline_def.stages if s.name == stage_name), None)
        if not stage_def:
            continue

        # Preflight check: Verify stage module exists before execution.
        # For Python stages: modules/{stage_name}.py
        # For Rust stages: modules/{stage_name}.rs (compiled at runtime in container)
        # This catches missing modules early, before Docker build/launch.
        runtime = stage_def.runtime  # Defaults to "python" in StageDef model
        if runtime == "rust":
            module_path = workspace_path / "modules" / f"{stage_name}.rs"
            if not module_path.exists():
                errors.append(f"Stage '{stage_name}': Rust module not found at modules/{stage_name}.rs")
        else:
            module_path = workspace_path / "modules" / f"{stage_name}.py"
            if not module_path.exists():
                errors.append(f"Stage '{stage_name}': Python module not found at modules/{stage_name}.py")

        # Check config file (optional but check for YAML errors)
        config_path = workspace_path / "configs" / f"{stage_name}.yaml"
        if config_path.exists():
            try:
                with open(config_path) as f:
                    yaml.safe_load(f)
            except yaml.YAMLError as e:
                errors.append(f"Stage '{stage_name}': config YAML error - {e}")
        else:
            warnings.append(f"Stage '{stage_name}': config file not found at configs/{stage_name}.yaml")

        # Backend-aware sanity checks for compute.profile (dry-run)
        if config:
            from goldfish.cloud.factory import backend_requires_compute_profile, validate_compute_profile

            if backend_requires_compute_profile(config):
                runtime_overrides: dict | None = None
                if config_override:
                    stage_overrides = config_override.get(stage_name)
                    if isinstance(stage_overrides, dict):
                        runtime_overrides = stage_overrides
                    else:
                        runtime_overrides = config_override

                merged_config = merge_stage_config(
                    stage_name=stage_name,
                    workspace_path=workspace_path,
                    runtime_overrides=runtime_overrides,
                )
                compute_cfg = merged_config.get("compute")
                profile = compute_cfg.get("profile") if isinstance(compute_cfg, dict) else None

                if not profile:
                    warnings.append(f"Stage '{stage_name}': no compute.profile specified for GCE backend")
                elif not isinstance(profile, str):
                    errors.append(f"Stage '{stage_name}': compute.profile must be a string")
                else:
                    ok, err = validate_compute_profile(config, profile)
                    if not ok:
                        errors.append(f"Stage '{stage_name}': compute.profile '{profile}' is invalid: {err}")

        # 2b. SVS Contract Validation (if SVS enabled)
        if config and config.svs.enabled:
            # Load and merge config to resolve params
            merged_config = merge_stage_config(
                stage_name=stage_name,
                workspace_path=workspace_path,
                runtime_overrides=config_override,  # Apply runtime overrides if provided
            )

            # Convert Pydantic model to dict for SVS contract validator
            # (SignalDef is used by parser, but svs.contract expects dict)
            stage_def_dict = {
                "name": stage_def.name,
                "outputs": {name: sig.model_dump() for name, sig in stage_def.outputs.items()},
            }

            contract_errors = validate_stage_contracts(stage_def_dict, merged_config)
            for err in contract_errors:
                errors.append(f"Stage '{stage_name}' contract: {err}")

        # Check inputs
        for input_name, input_def in stage_def.inputs.items():
            # Skip if overridden
            if inputs_override and input_name in inputs_override:
                continue

            # Check dataset inputs
            if input_def.type == "dataset" and input_def.dataset:
                dataset = db.get_source(input_def.dataset)
                if not dataset:
                    errors.append(f"Stage '{stage_name}': dataset '{input_def.dataset}' not found")
                elif config and config.svs.enabled and input_def.output_schema:
                    metadata, status = parse_source_metadata(dataset.get("metadata"))
                    if status == "ok" and metadata:
                        merged_config = merge_stage_config(
                            stage_name=stage_name,
                            workspace_path=workspace_path,
                            runtime_overrides=config_override,
                        )
                        try:
                            resolved_schema = resolve_config_params(input_def.output_schema, merged_config)
                        except ConfigParamNotFoundError as e:
                            param = e.details.get("param")
                            errors.append(f"Stage '{stage_name}' input '{input_name}': missing config param '{param}'")
                        else:
                            schema_errors = validate_input_schema_against_metadata(
                                input_name=input_name,
                                input_schema=resolved_schema,
                                metadata=metadata,
                            )
                            for err in schema_errors:
                                errors.append(f"Stage '{stage_name}': {err}")
                    elif status != "ok":
                        warnings.append(
                            f"Stage '{stage_name}' input '{input_name}': metadata {status}, skipping schema check"
                        )

            # Check from_stage inputs
            if input_def.from_stage:
                if input_def.from_stage not in all_stages:
                    errors.append(
                        f"Stage '{stage_name}': input '{input_name}' references "
                        f"unknown stage '{input_def.from_stage}'"
                    )

    return {
        "valid": len(errors) == 0,
        "stages_to_run": stages_to_run,
        "validation_errors": errors,
        "warnings": warnings,
    }
