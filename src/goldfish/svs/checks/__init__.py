"""SVS Checks - Mechanistic validation checks for stage outputs.

CheckResult is always available. Output check functions require numpy
and are only available in container environments.
"""

from goldfish.svs.checks.result import CheckResult


def __getattr__(name: str):
    """Lazy import for numpy-dependent output checks."""
    if name in ("check_entropy", "check_null_ratio", "check_top_k_concentration", "check_vocab_utilization"):
        from goldfish.svs.checks import output_checks

        return getattr(output_checks, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CheckResult",
    "check_entropy",
    "check_null_ratio",
    "check_top_k_concentration",
    "check_vocab_utilization",
]
