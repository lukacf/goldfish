"""YAML loader and validation helpers for launcher configs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import yaml


class ConfigError(ValueError):
    """Raised when a config fails validation."""


def load_config(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config not found: {p}")
    with p.open("r") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            raise ConfigError(f"Invalid YAML in {p}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"Config must be a mapping: {p}")
    data.setdefault("_path", str(p))
    return data


def _require_keys(cfg: Dict[str, Any], keys: list[str], context: str) -> None:
    for key in keys:
        if key not in cfg:
            raise ConfigError(f"Missing required key '{key}' in {context}")


def validate_source(cfg: Dict[str, Any]) -> None:
    _require_keys(cfg, ["kind", "name", "origin"], "source config")
    if cfg.get("kind") != "source":
        raise ConfigError("kind must be 'source' for source configs")


def validate_job(cfg: Dict[str, Any]) -> None:
    _require_keys(cfg, ["kind", "name", "inputs", "outputs", "compute", "image", "entrypoint"], "job config")
    if cfg.get("kind") != "job":
        raise ConfigError("kind must be 'job' for job configs")
    if not isinstance(cfg.get("inputs"), dict):
        raise ConfigError("inputs must be a mapping")
    if not isinstance(cfg.get("outputs"), dict):
        raise ConfigError("outputs must be a mapping")
    compute = cfg.get("compute")
    if not isinstance(compute, dict):
        raise ConfigError("compute must be a mapping")
    gpu = compute.get("gpu")
    if gpu not in (None, "none", "h100", "a100"):
        raise ConfigError("compute.gpu must be one of none|h100|a100")
    image = cfg.get("image")
    if isinstance(image, dict):
        if "uri" not in image:
            raise ConfigError("image uri is required when image is a mapping")
    elif not isinstance(image, str):
        raise ConfigError("image must be a string or mapping with uri")
    # inputs require 'from'
    for name, spec in cfg.get("inputs", {}).items():
        if "from" not in (spec or {}):
            raise ConfigError(f"input '{name}' missing 'from'")
    # outputs require sink and path
    for name, spec in cfg.get("outputs", {}).items():
        if not isinstance(spec, dict):
            raise ConfigError(f"output '{name}' must be a mapping")
        if "sink" not in spec or "path" not in spec:
            raise ConfigError(f"output '{name}' requires sink and path")


def resolve_input(input_spec: Dict[str, Any], project_root: Path) -> Dict[str, Any]:
    """Follow a `from:` reference to another config and return the loaded dict."""
    ref = input_spec.get("from")
    if not ref:
        raise ConfigError("input spec missing 'from'")
    ref_path = Path(ref)
    if ref_path.suffix != ".yaml":
        ref_path = ref_path.with_suffix(".yaml")
    if not ref_path.is_absolute():
        ref_path = project_root / ref_path
    cfg = load_config(ref_path)
    if cfg.get("kind") == "source":
        validate_source(cfg)
    elif cfg.get("kind") == "job":
        validate_job(cfg)
    else:
        raise ConfigError(f"Unknown kind '{cfg.get('kind')}' in {ref_path}")
    return cfg


def collect_env(
    job_config: Dict[str, Any],
    resolved_inputs: list[Dict[str, Any]],
    *,
    run_id: str | None = None,
) -> Dict[str, str]:
    """Merge env vars from upstream manifests then job env and IO/system vars."""
    merged: Dict[str, str] = {}

    def _merge_env(env_block: Dict[str, Any] | None) -> None:
        if not env_block:
            return
        for k, v in env_block.items():
            merged[k] = str(v)

    for cfg in resolved_inputs:
        state = cfg.get("state") or {}
        manifest_env = None
        if isinstance(state, dict):
            manifest = state.get("manifest") or {}
            manifest_env = manifest.get("env") if isinstance(manifest, dict) else None
            manifest_path = state.get("manifest_path")
            if not manifest_env and manifest_path:
                mp = Path(manifest_path)
                if mp.exists():
                    try:
                        manifest_data = yaml.safe_load(mp.read_text()) or {}
                        manifest_env = manifest_data.get("env")
                    except Exception:
                        manifest_env = None
        _merge_env(manifest_env)

    _merge_env(job_config.get("env"))

    # IO env vars
    for inp in job_config.get("inputs", {}):
        merged[f"INPUT_{inp.upper()}"] = f"/mnt/inputs/{inp}"
    for out in job_config.get("outputs", {}):
        merged[f"OUTPUT_{out.upper()}"] = f"/mnt/outputs/{out}"

    # System env vars
    job_name = job_config.get("name")
    if job_name:
        merged["JOB_NAME"] = str(job_name)
    if run_id:
        merged["RUN_ID"] = str(run_id)
    return merged
