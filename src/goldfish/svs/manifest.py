"""SVS manifest reading and aggregation.

This module reads and aggregates SVS manifest files from the outputs directory:
- svs_stats.json: Raw stats computed during stage execution
- svs_findings.json: AI review findings + updated stats

Key behaviors:
- Gracefully handles missing/corrupt manifests
- Version checking with backward compatibility
- Stats overlay (findings stats override base stats)
- Returns structured result with missing/version info
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Manifest version - bump when format changes
EXPECTED_MANIFEST_VERSION = 1


def read_svs_manifests(outputs_dir: Path) -> dict[str, Any]:
    """Read and aggregate SVS manifests from outputs directory.

    Manifest precedence (later wins for stats):
    1. svs_stats.json - raw stats (written first)
    2. svs_findings.json - stats + AI review (written last)

    Args:
        outputs_dir: Path to stage outputs directory

    Returns:
        Dict with:
        - stats: Aggregated stats from both manifests
        - ai_review: AI review findings (or None if not present)
        - missing: List of missing/corrupt manifest names
        - version: Manifest version (from most recent valid manifest)
        - version_mismatch: True if version doesn't match expected
    """
    result: dict[str, Any] = {
        "stats": {},
        "ai_review": None,
        "during_run": None,
        "missing": [],
        "version": None,
        "version_mismatch": False,
    }

    goldfish_dir = outputs_dir / ".goldfish"
    if not goldfish_dir.exists():
        result["missing"].append(".goldfish directory")
        return result

    # 1. Read stats manifest (base layer)
    stats_path = goldfish_dir / "svs_stats.json"
    if stats_path.exists():
        try:
            data = json.loads(stats_path.read_text())
            version = data.get("version", 0)
            if version != EXPECTED_MANIFEST_VERSION:
                logger.warning(f"Stats manifest version mismatch: {version} != {EXPECTED_MANIFEST_VERSION}")
                result["version_mismatch"] = True
            result["stats"] = data.get("stats", {})
            result["version"] = version
        except json.JSONDecodeError as e:
            logger.error(f"Corrupt svs_stats.json: {e}")
            result["missing"].append("svs_stats.json (corrupt)")
    else:
        result["missing"].append("svs_stats.json")

    # 2. Read findings manifest (overlay - takes precedence)
    findings_path = goldfish_dir / "svs_findings.json"
    if findings_path.exists():
        try:
            data = json.loads(findings_path.read_text())
            version = data.get("version", 0)
            if version != EXPECTED_MANIFEST_VERSION:
                logger.warning(f"Findings manifest version mismatch: {version} != {EXPECTED_MANIFEST_VERSION}")
                result["version_mismatch"] = True

            # Extract during-run history if present
            history = data.get("history")
            if isinstance(history, list) and history:
                # Compute decision based on severity in history
                severity_rank = {
                    "BLOCK": 2,
                    "ERROR": 2,
                    "WARN": 1,
                    "WARNING": 1,
                }
                decision = "approved"
                for entry in history:
                    severity = str(entry.get("severity", "")).upper()
                    if severity_rank.get(severity, 0) == 2:
                        decision = "blocked"
                        break
                    if severity_rank.get(severity, 0) == 1 and decision != "blocked":
                        decision = "warned"
                result["during_run"] = {
                    "decision": decision,
                    "history": history,
                }

            # Extract AI review info (exclude during-run findings if tagged)
            findings_list = data.get("findings", [])
            if isinstance(findings_list, list):
                filtered_findings = [f for f in findings_list if "[during_run]" not in str(f)]
            else:
                filtered_findings = []
            result["ai_review"] = {
                "decision": data.get("decision"),
                "findings": filtered_findings,
                "duration_ms": data.get("duration_ms"),
            }

            # Findings stats OVERRIDE base stats (per-signal merge)
            findings_stats = data.get("stats", {})
            for signal_name, signal_stats in findings_stats.items():
                if signal_name in result["stats"]:
                    # Merge: findings stats override base stats for same keys
                    result["stats"][signal_name].update(signal_stats)
                else:
                    result["stats"][signal_name] = signal_stats

            result["version"] = version
        except json.JSONDecodeError as e:
            logger.error(f"Corrupt svs_findings.json: {e}")
            result["missing"].append("svs_findings.json (corrupt)")
    else:
        result["missing"].append("svs_findings.json")

    return result
