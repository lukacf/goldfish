"""Pipeline parser and validator."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from goldfish.errors import GoldfishError
from goldfish.models import PipelineDef, SignalDef, StageDef


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
                        if isinstance(item, str):
                            item = {"name": item, "type": "dataset"}
                        elif isinstance(item, dict):
                            item = dict(item)  # Copy to avoid mutation
                            if "type" not in item:
                                item["type"] = "dataset"
                            if "name" not in item:
                                # Generate name from dataset or use index
                                item["name"] = item.get("dataset", f"input_{index}")
                        return item

                    def _normalize_output_item(item, index: int = 0):
                        if isinstance(item, str):
                            item = {"name": item, "type": "directory"}
                        elif isinstance(item, dict):
                            item = dict(item)  # Copy to avoid mutation
                            if "type" not in item:
                                item["type"] = "directory"
                            if "name" not in item:
                                item["name"] = f"output_{index}"
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
                    stages.append(StageDef(**stage_data))
                data["stages"] = stages

            return PipelineDef(**data)
        except ValidationError as e:
            raise PipelineValidationError(f"Invalid pipeline schema: {e}") from e

    def validate(
        self,
        pipeline: PipelineDef,
        workspace_path: Path,
        dataset_exists_fn: Callable[[str], bool] | None = None,
    ) -> list[str]:
        """Validate pipeline definition.

        Checks:
        - Stage files exist (modules/{stage}.py, configs/{stage}.yaml)
        - Signal types match (output type == input type)
        - No circular dependencies
        - Datasets exist in registry (if dataset_exists_fn provided)
        - Input signals reference valid sources

        Args:
            pipeline: Pipeline definition
            workspace_path: Path to workspace directory
            dataset_exists_fn: Optional function to check if dataset exists

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Track available signals as we process stages
        available_signals: dict[str, SignalDef] = {}

        for stage in pipeline.stages:
            # Check stage files exist
            module_path = workspace_path / "modules" / f"{stage.name}.py"
            config_path = workspace_path / "configs" / f"{stage.name}.yaml"

            if not module_path.exists():
                errors.append(f"Module not found for stage '{stage.name}': {module_path}")
            if not config_path.exists():
                errors.append(f"Config not found for stage '{stage.name}': {config_path}")

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
                else:
                    # Input must be either dataset or from_stage
                    errors.append(
                        f"Stage '{stage.name}' input '{input_name}': must specify either 'dataset' or 'from_stage'"
                    )

            # Register outputs as available for next stages
            for output_name, output_def in stage.outputs.items():
                signal_ref = f"{stage.name}.{output_name}"
                available_signals[signal_ref] = output_def

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

            if stage.inputs:
                stage_data["inputs"] = {
                    name: {k: v for k, v in sig.model_dump().items() if v is not None and k != "name"}
                    for name, sig in stage.inputs.items()
                }

            if stage.outputs:
                stage_data["outputs"] = {
                    name: {k: v for k, v in sig.model_dump().items() if v is not None and k != "name"}
                    for name, sig in stage.outputs.items()
                }

            stages_list.append(stage_data)

        data: dict[str, Any] = {
            "name": pipeline.name,
            "description": pipeline.description,
            "stages": stages_list,
        }

        return yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
