"""Configuration validation utilities.

Provides validation for goldfish.yaml, pipeline.yaml, and stage configs.
"""

from pathlib import Path

import yaml


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
                    with open(config_file) as f:
                        yaml.safe_load(f)
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
