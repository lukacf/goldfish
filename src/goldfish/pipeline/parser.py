"""Pipeline parser and validator."""

from pathlib import Path
from typing import Optional

import yaml
from pydantic import ValidationError

from goldfish.errors import GoldfishError
from goldfish.models import PipelineDef, StageDef, SignalDef


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
                    # Convert input/output dicts to SignalDef objects
                    # Handle both dict format: {name: {type: ...}} and list format: [{name: ..., type: ...}]
                    if "inputs" in stage_data:
                        inputs_raw = stage_data["inputs"]
                        if isinstance(inputs_raw, list):
                            # Convert list to dict: [{name: "x", ...}] -> {"x": SignalDef(...)}
                            stage_data["inputs"] = {
                                item["name"]: SignalDef(**item)
                                for item in inputs_raw
                            }
                        elif isinstance(inputs_raw, dict):
                            # Dict format: {name: {type: ...}} -> {name: SignalDef(name=name, ...)}
                            stage_data["inputs"] = {
                                name: SignalDef(name=name, **sig_data)
                                for name, sig_data in inputs_raw.items()
                            }
                        else:
                            stage_data["inputs"] = {}
                    if "outputs" in stage_data:
                        outputs_raw = stage_data["outputs"]
                        if isinstance(outputs_raw, list):
                            # Convert list to dict: [{name: "y", ...}] -> {"y": SignalDef(...)}
                            stage_data["outputs"] = {
                                item["name"]: SignalDef(**item)
                                for item in outputs_raw
                            }
                        elif isinstance(outputs_raw, dict):
                            # Dict format: {name: {type: ...}} -> {name: SignalDef(name=name, ...)}
                            stage_data["outputs"] = {
                                name: SignalDef(name=name, **sig_data)
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
        dataset_exists_fn: Optional[callable] = None,
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
                if input_def.type == "dataset":
                    # Check dataset exists
                    if not input_def.dataset:
                        errors.append(
                            f"Stage '{stage.name}' input '{input_name}': "
                            f"dataset type requires 'dataset' field"
                        )
                    elif dataset_exists_fn and not dataset_exists_fn(input_def.dataset):
                        errors.append(
                            f"Stage '{stage.name}' input '{input_name}': "
                            f"dataset '{input_def.dataset}' not found"
                        )
                elif input_def.from_stage:
                    # Check signal from previous stage
                    signal_ref = f"{input_def.from_stage}.{input_name}"
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
                                f"does not produce output '{input_name}'"
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
                else:
                    # Input must be either dataset or from_stage
                    errors.append(
                        f"Stage '{stage.name}' input '{input_name}': "
                        f"must specify either 'dataset' or 'from_stage'"
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
                    errors.append(
                        f"Stage '{stage.name}' has circular dependency (references itself)"
                    )

        return errors

    def serialize(self, pipeline: PipelineDef) -> str:
        """Serialize pipeline to YAML string.

        Args:
            pipeline: Pipeline definition

        Returns:
            YAML string
        """
        # Convert to dict for YAML serialization
        data = {
            "name": pipeline.name,
            "description": pipeline.description,
            "stages": [],
        }

        for stage in pipeline.stages:
            stage_data = {"name": stage.name}

            if stage.inputs:
                stage_data["inputs"] = {
                    name: {
                        k: v
                        for k, v in sig.model_dump().items()
                        if v is not None and k != "name"
                    }
                    for name, sig in stage.inputs.items()
                }

            if stage.outputs:
                stage_data["outputs"] = {
                    name: {
                        k: v
                        for k, v in sig.model_dump().items()
                        if v is not None and k != "name"
                    }
                    for name, sig in stage.outputs.items()
                }

            data["stages"].append(stage_data)

        return yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
