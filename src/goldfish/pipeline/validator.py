"""Pipeline validation for dry-run mode.

Validates pipeline configuration without launching.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Optional

import yaml

from goldfish.db.database import Database
from goldfish.pipeline.parser import PipelineNotFoundError, PipelineParser
from goldfish.svs.contract import merge_stage_config, validate_stage_contracts

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

        # Check module exists
        module_path = workspace_path / "modules" / f"{stage_name}.py"
        if not module_path.exists():
            errors.append(f"Stage '{stage_name}': module not found at modules/{stage_name}.py")

        # Check config file (optional but check for YAML errors)
        config_path = workspace_path / "configs" / f"{stage_name}.yaml"
        if config_path.exists():
            try:
                with open(config_path) as f:
                    yaml.safe_load(f)
            except yaml.YAMLError as e:
                errors.append(f"Stage '{stage_name}': config YAML error - {e}")

        # 2b. SVS Contract Validation (if SVS enabled)
        if config and config.svs.enabled:
            # Load and merge config to resolve params
            merged_config = merge_stage_config(
                stage_name=stage_name,
                workspace_path=workspace_path,
                runtime_overrides=None,  # No overrides in dry-run
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
