"""Job conversion utilities.

Shared utility for converting database job dictionaries to JobInfo model objects.
"""

from goldfish.db.database import Database
from goldfish.models import JobInfo
from goldfish.utils import parse_datetime, parse_optional_datetime


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
