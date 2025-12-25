"""Pattern extraction for self-learning failure detection.

This module provides functionality to extract structured failure patterns
from stage run failures using AI analysis.
"""

from goldfish.svs.patterns.extractor import (
    FailurePattern,
    PatternExtractionError,
    RateLimitExceededError,
    extract_failure_pattern,
)

__all__ = [
    "FailurePattern",
    "extract_failure_pattern",
    "PatternExtractionError",
    "RateLimitExceededError",
]
