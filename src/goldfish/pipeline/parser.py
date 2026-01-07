"""Pipeline parser and validator."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from goldfish.errors import ConfigParamNotFoundError, GoldfishError
from goldfish.models import PipelineDef, SignalDef, StageDef
from goldfish.svs.contract import (
    merge_stage_config,
    resolve_config_params,
    validate_cross_stage_shapes,
    validate_input_schema_against_metadata,
    validate_stage_config_schema,
    validate_stage_contracts,
)


def _entrypoint_path_error(entrypoint: str) -> bool:
    path = Path(entrypoint)
    if path.is_absolute():
        return True
    if ".." in path.parts:
        return True
    if not path.parts or path.parts[0] != "entrypoints":
        return True
    return False


class PipelineValidationError(GoldfishError):
    """Pipeline validation failed."""

    pass


class PipelineNotFoundError(GoldfishError):
    """Pipeline file not found."""

    pass


class PipelineParser:
    """Parse and validate pipeline.yaml files."""

    def parse(self, yaml_path: Path) -> PipelineDef:
        """Parse pipeline.yaml into structured definition.

        Args:
            yaml_path: Path to pipeline.yaml

        Returns:
            PipelineDef object

        Raises:
            PipelineNotFoundError: If file doesn't exist
            PipelineValidationError: If YAML is invalid or doesn't match schema
        """
        if not yaml_path.exists():
            raise PipelineNotFoundError(f"Pipeline file not found: {yaml_path}")

        try:
            with open(yaml_path) as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise PipelineValidationError(f"Invalid YAML: {e}") from e

        if not data:
            raise PipelineValidationError("Pipeline file is empty")

        try:
            # Convert dict stages to StageDef objects
            if "stages" in data:
                stages = []
                for stage_data in data["stages"]:

                    def _normalize_input_item(item, index: int = 0):
                        schema_present = False
                        if isinstance(item, str):
                            item = {"name": item, "type": "dataset"}
                        elif isinstance(item, dict):
                            schema_present = "schema" in item
                            item = dict(item)  # Copy to avoid mutation
                            if "type" not in item:
                                item["type"] = "dataset"
                            if "name" not in item:
                                # Generate name from dataset or use index
                                item["name"] = item.get("dataset", f"input_{index}")
                        item["schema_present"] = schema_present
                        return item

                    def _normalize_output_item(item, index: int = 0):
                        schema_present = False
                        if isinstance(item, str):
                            item = {"name": item, "type": "directory"}
                        elif isinstance(item, dict):
                            schema_present = "schema" in item
                            item = dict(item)  # Copy to avoid mutation
                            if "type" not in item:
                                item["type"] = "directory"
                            if "name" not in item:
                                item["name"] = f"output_{index}"
                        item["schema_present"] = schema_present
                        return item

                    # Convert input/output dicts to SignalDef objects
                    # Handle both dict format: {name: {type: ...}} and list format: [{name: ..., type: ...}]
                    if "inputs" in stage_data:
                        inputs_raw = stage_data["inputs"]
                        if isinstance(inputs_raw, list):
                            # Convert list to dict: [{name: "x", ...}] -> {"x": SignalDef(...)}
                            normalized_inputs = [_normalize_input_item(item, i) for i, item in enumerate(inputs_raw)]
                            stage_data["inputs"] = {item["name"]: SignalDef(**item) for item in normalized_inputs}
                        elif isinstance(inputs_raw, dict):
                            # Dict format: {name: {type: ...}} -> {name: SignalDef(name=name, ...)}
                            stage_data["inputs"] = {
                                name: SignalDef(
                                    name=name,
                                    **{
                                        k: v
                                        for k, v in _normalize_input_item(
                                            {"name": name, "type": "dataset", "dataset": sig_data}
                                            if isinstance(sig_data, str)
                                            else {"name": name, **sig_data}
                                        ).items()
                                        if k != "name"
                                    },
                                )
                                for name, sig_data in inputs_raw.items()
                            }
                        else:
                            stage_data["inputs"] = {}
                    if "outputs" in stage_data:
                        outputs_raw = stage_data["outputs"]
                        if isinstance(outputs_raw, list):
                            # Convert list to dict: [{name: "y", ...}] -> {"y": SignalDef(...)}
                            normalized_outputs = [_normalize_output_item(item, i) for i, item in enumerate(outputs_raw)]
                            stage_data["outputs"] = {item["name"]: SignalDef(**item) for item in normalized_outputs}
                        elif isinstance(outputs_raw, dict):
                            # Dict format: {name: {type: ...}} -> {name: SignalDef(name=name, ...)}
                            stage_data["outputs"] = {
                                name: SignalDef(
                                    name=name,
                                    **{
                                        k: v
                                        for k, v in _normalize_output_item(
                                            {"name": name, "type": "directory"}
                                            if not isinstance(sig_data, dict)
                                            else {"name": name, **sig_data}
                                        ).items()
                                        if k != "name"
                                    },
                                )
                                for name, sig_data in outputs_raw.items()
                            }
                        else:
                            stage_data["outputs"] = {}
                    stage_def = StageDef(**stage_data)

                    # Enforce schema tag presence (schema can be null, but tag must exist)
                    for input_name, input_def in stage_def.inputs.items():
                        if not input_def.schema_present:
                            raise PipelineValidationError(
                                f"Stage '{stage_def.name}' input '{input_name}': schema is required "
                                "(use schema: null to opt out)."
                            )
                    for output_name, output_def in stage_def.outputs.items():
                        if not output_def.schema_present:
                            raise PipelineValidationError(
                                f"Stage '{stage_def.name}' output '{output_name}': schema is required "
                                "(use schema: null to opt out)."
                            )

                    stages.append(stage_def)
                data["stages"] = stages

            return PipelineDef(**data)
        except ValidationError as e:
            raise PipelineValidationError(f"Invalid pipeline schema: {e}") from e

    def validate(
        self,
        pipeline: PipelineDef,
        workspace_path: Path,
        dataset_exists_fn: Callable[[str], bool] | None = None,
        dataset_metadata_fn: Callable[[str], tuple[dict | None, str] | None] | None = None,
    ) -> list[str]:
        """Validate pipeline definition.

        Checks:
        - Stage files exist (modules/{stage}.py or modules/{stage}.rs, configs/{stage}.yaml)
        - Signal types match (output type == input type)
        - No circular dependencies
        - Datasets exist in registry (if dataset_exists_fn provided)
        - Input signals reference valid sources

        Args:
            pipeline: Pipeline definition
            workspace_path: Path to workspace directory
            dataset_exists_fn: Optional function to check if dataset exists
            dataset_metadata_fn: Optional function returning (metadata, status) for dataset

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Track available signals as we process stages
        available_signals: dict[str, SignalDef] = {}

        for stage in pipeline.stages:
            runtime = stage.runtime or "python"
            if runtime not in {"python", "rust"}:
                errors.append(f"Stage '{stage.name}': runtime '{runtime}' unsupported (use 'python' or 'rust')")

            # Check module file exists (required)
            if runtime == "rust":
                module_path = workspace_path / "modules" / f"{stage.name}.rs"
                if not module_path.exists():
                    errors.append(f"Module not found for stage '{stage.name}': {module_path}")

                if stage.entrypoint and _entrypoint_path_error(stage.entrypoint):
                    errors.append(f"Stage '{stage.name}': entrypoint must be within entrypoints/ and relative")
            else:
                module_path = workspace_path / "modules" / f"{stage.name}.py"
                if not module_path.exists():
                    errors.append(f"Module not found for stage '{stage.name}': {module_path}")
            # Note: Config files (configs/{stage}.yaml) are optional - merge_stage_config handles them

            # SVS Preflight: Resolve config and validate types (if declared)
            stage_config = merge_stage_config(stage.name, workspace_path)
            errors.extend(validate_stage_config_schema(stage.name, stage.config_schema, stage_config))

            # SVS Preflight: Validate output schemas with config resolution
            stage_def_dict = {
                "outputs": {
                    name: {"schema": output_def.output_schema}
                    for name, output_def in stage.outputs.items()
                    if output_def.output_schema
                }
            }
            contract_errors = validate_stage_contracts(stage_def_dict, stage_config)
            for err in contract_errors:
                errors.append(f"Stage '{stage.name}': {err}")

            # Validate inputs
            for input_name, input_def in stage.inputs.items():
                # Check from_stage FIRST - it takes precedence over type
                # This allows dataset-type signals to be passed between stages
                if input_def.from_stage:
                    # Check signal from previous stage
                    signal_name = input_def.signal or input_name
                    signal_ref = f"{input_def.from_stage}.{signal_name}"
                    if signal_ref not in available_signals:
                        # Check if the stage exists
                        stage_names = [s.name for s in pipeline.stages]
                        if input_def.from_stage not in stage_names:
                            errors.append(
                                f"Stage '{stage.name}' input '{input_name}': "
                                f"references unknown stage '{input_def.from_stage}'"
                            )
                        else:
                            errors.append(
                                f"Stage '{stage.name}' input '{input_name}': "
                                f"signal '{signal_ref}' not found. Stage '{input_def.from_stage}' "
                                f"does not produce output '{signal_name}'"
                            )
                    else:
                        # Check type compatibility
                        source_signal = available_signals[signal_ref]
                        if source_signal.type != input_def.type:
                            errors.append(
                                f"Stage '{stage.name}' input '{input_name}': "
                                f"type mismatch. Expected '{input_def.type}', "
                                f"got '{source_signal.type}' from '{signal_ref}'"
                            )
                elif input_def.type == "dataset":
                    # External dataset (not from previous stage)
                    # Check dataset exists
                    if not input_def.dataset:
                        errors.append(
                            f"Stage '{stage.name}' input '{input_name}': dataset type requires 'dataset' field"
                        )
                    elif dataset_exists_fn and not dataset_exists_fn(input_def.dataset):
                        errors.append(
                            f"Stage '{stage.name}' input '{input_name}': dataset '{input_def.dataset}' not found"
                        )
                    elif input_def.output_schema and dataset_metadata_fn:
                        dataset_meta = dataset_metadata_fn(input_def.dataset)
                        if dataset_meta:
                            metadata, status = dataset_meta
                            if status == "ok" and metadata:
                                try:
                                    resolved_schema = resolve_config_params(input_def.output_schema, stage_config)
                                except ConfigParamNotFoundError as e:
                                    param = e.details["param"]
                                    errors.append(
                                        f"Stage '{stage.name}' input '{input_name}': missing config param '{param}'"
                                    )
                                else:
                                    schema_errors = validate_input_schema_against_metadata(
                                        input_name=input_name,
                                        input_schema=resolved_schema,
                                        metadata=metadata,
                                    )
                                    for err in schema_errors:
                                        errors.append(f"Stage '{stage.name}': {err}")
                else:
                    # Input must be either dataset or from_stage
                    errors.append(
                        f"Stage '{stage.name}' input '{input_name}': must specify either 'dataset' or 'from_stage'"
                    )

            # Register outputs as available for next stages
            for output_name, output_def in stage.outputs.items():
                signal_ref = f"{stage.name}.{output_name}"
                available_signals[signal_ref] = output_def

        # Cross-stage schema compatibility (inputs vs upstream outputs)
        errors.extend(validate_cross_stage_shapes(pipeline, workspace_path))

        # Check for circular dependencies (basic check)
        # More sophisticated check would require topological sort
        for stage in pipeline.stages:
            for input_def in stage.inputs.values():
                if input_def.from_stage == stage.name:
                    errors.append(f"Stage '{stage.name}' has circular dependency (references itself)")

        return errors

    def serialize(self, pipeline: PipelineDef) -> str:
        """Serialize pipeline to YAML string.

        Args:
            pipeline: Pipeline definition

        Returns:
            YAML string
        """
        # Convert to dict for YAML serialization
        stages_list: list[dict[str, Any]] = []

        for stage in pipeline.stages:
            stage_data: dict[str, Any] = {"name": stage.name}

            if stage.runtime:
                stage_data["runtime"] = stage.runtime
            if stage.entrypoint:
                stage_data["entrypoint"] = stage.entrypoint

            if stage.inputs:
                stage_data["inputs"] = {
                    name: {
                        k: v
                        for k, v in sig.model_dump(exclude_none=False, by_alias=True).items()
                        if k not in {"name", "schema_present"}
                    }
                    for name, sig in stage.inputs.items()
                }

            if stage.outputs:
                stage_data["outputs"] = {
                    name: {
                        k: v
                        for k, v in sig.model_dump(exclude_none=False, by_alias=True).items()
                        if k not in {"name", "schema_present"}
                    }
                    for name, sig in stage.outputs.items()
                }

            stages_list.append(stage_data)

        data: dict[str, Any] = {
            "name": pipeline.name,
            "description": pipeline.description,
            "stages": stages_list,
        }

        return yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
