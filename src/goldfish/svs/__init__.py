"""Goldfish Semantic Validation System (SVS).

A hybrid mechanistic + AI validation system that catches silent failures
in ML experiments across three phases: pre-run, during-run, and post-run.

Key concepts:
- Schema as Contract (Law): Pipeline schema is authoritative
- Hierarchy of Truth: Mechanistic checks always enforced, AI can only escalate
- Preflight Contract Check: Validate schemas BEFORE container build
"""

from goldfish.svs.config import SVSConfig

__all__ = ["SVSConfig"]
