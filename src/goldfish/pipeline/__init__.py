"""Pipeline management for Goldfish.

This module provides tools for:
- Parsing pipeline.yaml definitions
- Validating pipeline structure and dependencies
- Managing pipeline updates
"""

from goldfish.pipeline.manager import PipelineManager
from goldfish.pipeline.parser import PipelineParser

__all__ = ["PipelineManager", "PipelineParser"]
