"""Job conversion utilities.

Shared utility for converting database job dictionaries to JobInfo model objects.
"""

from goldfish.db.database import Database
from goldfish.models import JobInfo, StageRunInfo
from goldfish.utils import parse_datetime, parse_optional_datetime
import json


def job_dict_to_info(job: dict, db: Database) -> JobInfo:
    """Convert database job dict to JobInfo model.

    Args:
        job: Job dictionary from database
        db: Database instance for fetching job inputs

    Returns:
        JobInfo model with all fields populated
    """
    job_inputs = db.get_job_inputs(job["id"])
    input_sources = [inp["source_name"] for inp in job_inputs]

    return JobInfo(
        job_id=job["id"],
        status=job["status"],
        workspace=job["workspace"],
        snapshot_id=job["snapshot_id"],
        script=job["script"],
        started_at=parse_datetime(job["started_at"]),
        completed_at=parse_optional_datetime(job.get("completed_at")),
        log_uri=job.get("log_uri"),
        artifact_uri=job.get("artifact_uri"),
        error=job.get("error"),
        input_sources=input_sources,
    )


def stage_run_dict_to_info(row: dict) -> StageRunInfo:
    """Convert database stage_run row dict to StageRunInfo model."""

    def _safe_json(val):
        if not val:
            return None
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return None

    # Lazy JSON accessors (cached on the model) to avoid repeated parsing
    hints_raw = row.get("hints_json")
    outputs_raw = row.get("outputs_json")
    config_raw = row.get("config_json")
    inputs_raw = row.get("inputs_json")

    return StageRunInfo(
        stage_run_id=row.get("id") or row.get("stage_run_id"),
        pipeline_run_id=row.get("pipeline_run_id"),
        workspace=row["workspace_name"],
        pipeline=row.get("pipeline_name"),
        version=row["version"],
        stage=row["stage_name"],
        status=row["status"],
        started_at=parse_optional_datetime(row.get("started_at")),
        completed_at=parse_optional_datetime(row.get("completed_at")),
        progress=row.get("progress"),
        log_uri=row.get("log_uri"),
        artifact_uri=row.get("artifact_uri"),
        profile=row.get("profile"),
        hints=_safe_json(hints_raw),
        outputs=_safe_json(outputs_raw),
        config=_safe_json(config_raw),
        inputs=_safe_json(inputs_raw),
        error=row.get("error"),
    )
