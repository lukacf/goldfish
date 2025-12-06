"""Append-only JSONL run registry."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class RunRegistry:
    path: Path

    def _append(self, record: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a") as f:
            f.write(json.dumps(record) + "\n")

    def log_started(self, run_id: str, job_path: str, backend: str, **metadata: Any) -> None:
        rec = {
            "id": run_id,
            "event": "started",
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "job": job_path,
            "backend": backend,
        }
        rec.update(metadata)
        self._append(rec)

    def log_completed(self, run_id: str, outputs: List[Dict[str, Any]], duration_sec: float, **meta: Any) -> None:
        rec = {
            "id": run_id,
            "event": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "duration_sec": duration_sec,
            "outputs": outputs,
        }
        rec.update(meta)
        self._append(rec)

    def log_failed(self, run_id: str, error: str, exit_code: int, duration_sec: float, job_path: str | None = None) -> None:
        self._append(
            {
                "id": run_id,
                "event": "failed",
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "duration_sec": duration_sec,
                "error": error,
                "exit_code": exit_code,
                **({"job": job_path} if job_path else {}),
            }
        )

    def log_aborted(self, run_id: str, signal: str, duration_sec: float) -> None:
        self._append(
            {
                "id": run_id,
                "event": "aborted",
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "duration_sec": duration_sec,
                "signal": signal,
            }
        )

    def _read_all(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        with self.path.open() as f:
            return [json.loads(line) for line in f if line.strip()]

    def list_runs(self, limit: int = 20, status_filter: Optional[str] = None, job_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = self._read_all()
        if status_filter:
            rows = [r for r in rows if r.get("event") == status_filter]
        if job_filter:
            rows = [r for r in rows if job_filter in r.get("job", "")]
        rows = list(reversed(rows))
        return rows[:limit]

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        for row in reversed(self._read_all()):
            if row.get("id") == run_id:
                return row
        return None

    def get_latest_for_job(self, job_path: str) -> Optional[Dict[str, Any]]:
        for row in reversed(self._read_all()):
            if row.get("job") == job_path:
                return row
        return None
