"""GoldfishSettings representation contract tests."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Literal, get_args, get_origin, get_type_hints


def test_goldfish_settings_when_defined_is_frozen_dataclass_with_required_fields() -> None:
    """GoldfishSettings is immutable and exposes core config fields."""

    from goldfish.config.settings import GoldfishSettings

    assert dataclasses.is_dataclass(GoldfishSettings)
    assert GoldfishSettings.__dataclass_params__.frozen is True

    assert [f.name for f in dataclasses.fields(GoldfishSettings)] == [
        "project_name",
        "dev_repo_path",
        "workspaces_path",
        "backend",
        "db_path",
        "db_backend",
        "log_format",
        "log_level",
        "stage_timeout",
        "gce_launch_timeout",
    ]

    hints = get_type_hints(GoldfishSettings)
    assert hints["project_name"] is str
    assert hints["dev_repo_path"] is Path
    assert hints["workspaces_path"] is Path
    assert get_origin(hints["backend"]) is Literal
    assert get_args(hints["backend"]) == ("local", "gce")
    assert hints["db_path"] is Path
    assert get_origin(hints["db_backend"]) is Literal
    assert get_args(hints["db_backend"]) == ("sqlite", "postgres")
    assert get_origin(hints["log_format"]) is Literal
    assert get_args(hints["log_format"]) == ("json", "console")
    assert hints["log_level"] is str
    assert hints["stage_timeout"] is int
    assert hints["gce_launch_timeout"] is int
