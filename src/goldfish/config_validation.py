"""Configuration validation utilities.

Provides validation for goldfish.yaml, pipeline.yaml, and stage configs.
"""

from dataclasses import dataclass, field
from difflib import get_close_matches
from pathlib import Path

import yaml


@dataclass
class StageConfigValidationResult:
    """Result of validating a single stage config file."""

    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


_DEFAULT_STAGE_CONFIG_FIELDS = {
    # Common model/training knobs (heuristic; stage configs are intentionally flexible)
    "freeze_backbone",
    "learning_rate",
    "lr",
    "batch_size",
    "epochs",
    "seed",
    "weight_decay",
    "dropout",
    "optimizer",
    "scheduler",
}


def validate_stage_config(path: Path) -> StageConfigValidationResult:
    """Validate a stage config YAML for common typos.

    Stage configs are user-defined and intentionally flexible, so this validator
    is warning-oriented: it flags suspicious keys that look like typos of common
    fields (e.g., `freeze_backone` vs `freeze_backbone`).
    """
    errors: list[str] = []
    warnings: list[str] = []

    try:
        data = yaml.safe_load(path.read_text())
    except OSError as exc:
        return StageConfigValidationResult(valid=False, errors=[str(exc)], warnings=[])
    except yaml.YAMLError as exc:
        return StageConfigValidationResult(valid=False, errors=[f"YAML syntax error - {exc}"], warnings=[])

    if data is None:
        return StageConfigValidationResult(valid=True, errors=[], warnings=[])

    if not isinstance(data, dict):
        return StageConfigValidationResult(valid=False, errors=["Stage config must be a YAML mapping"], warnings=[])

    for key in data:
        if not isinstance(key, str):
            continue
        if key in _DEFAULT_STAGE_CONFIG_FIELDS:
            continue

        match = get_close_matches(key, _DEFAULT_STAGE_CONFIG_FIELDS, n=1, cutoff=0.8)
        if match:
            warnings.append(f"Unknown field '{key}'. Did you mean '{match[0]}'?")

    return StageConfigValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)


def validate_project_config(
    project_root: Path,
    workspace_path: Path | None = None,
    workspace_name: str | None = None,
) -> dict:
    """Validate configuration files for typos and errors.

    Args:
        project_root: Path to project root (containing goldfish.yaml)
        workspace_path: Optional path to workspace directory
        workspace_name: Optional workspace name (for error messages)

    Returns:
        dict with:
        - valid: bool - True if all validations pass
        - errors: list - Critical issues that must be fixed
        - warnings: list - Non-critical issues (suggestions)
        - files_checked: list - Which files were validated
    """
    errors: list[str] = []
    warnings: list[str] = []
    files_checked: list[str] = []

    # 1. Validate goldfish.yaml
    config_path = project_root / "goldfish.yaml"
    if config_path.exists():
        files_checked.append("goldfish.yaml")
        try:
            # Try loading - this will catch unknown fields and type errors
            from goldfish.config import GoldfishConfig

            GoldfishConfig.load(project_root)
        except Exception as e:
            errors.append(f"goldfish.yaml: {e}")

    # 2. Validate workspace configs if specified
    if workspace_path:
        ws_name = workspace_name or workspace_path.name

        # Validate pipeline.yaml
        pipeline_path = workspace_path / "pipeline.yaml"
        if pipeline_path.exists():
            files_checked.append(f"{ws_name}/pipeline.yaml")
            try:
                from goldfish.pipeline.parser import PipelineParser

                parser = PipelineParser()
                parser.parse(pipeline_path)
            except Exception as e:
                errors.append(f"pipeline.yaml: {e}")

        # Validate configs/*.yaml (syntax check)
        configs_dir = workspace_path / "configs"
        if configs_dir.exists():
            for config_file in configs_dir.glob("*.yaml"):
                files_checked.append(f"{ws_name}/configs/{config_file.name}")
                try:
                    result = validate_stage_config(config_file)
                    errors.extend([f"configs/{config_file.name}: {e}" for e in result.errors])
                    warnings.extend([f"configs/{config_file.name}: {w}" for w in result.warnings])
                except yaml.YAMLError as e:
                    errors.append(f"configs/{config_file.name}: YAML syntax error - {e}")

        # Check that modules exist for each pipeline stage
        if pipeline_path.exists():
            try:
                from goldfish.pipeline.parser import PipelineParser

                parser = PipelineParser()
                pipeline_def = parser.parse(pipeline_path)
                for stage in pipeline_def.stages:
                    runtime = stage.runtime or "python"
                    if runtime == "rust":
                        module_path = workspace_path / "modules" / f"{stage.name}.rs"
                        if not module_path.exists():
                            warnings.append(f"Stage '{stage.name}': module not found at modules/{stage.name}.rs")
                    else:
                        module_path = workspace_path / "modules" / f"{stage.name}.py"
                        if not module_path.exists():
                            warnings.append(f"Stage '{stage.name}': module not found at modules/{stage.name}.py")
            except Exception:
                pass  # Already reported in pipeline validation

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "files_checked": files_checked,
    }
