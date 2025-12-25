"""SVS Output Checks - Mechanistic validation of stage outputs.

These checks run on output data to catch common data quality issues:
- Entropy: Measures information content (low entropy = degenerate outputs)
- Null ratio: Detects excessive missing values
- Vocab utilization: Ensures model uses vocabulary breadth (for token data)
- Top-k concentration: Detects over-reliance on few values

All checks are mechanistic (no AI) and return structured results.
"""

from dataclasses import dataclass

import numpy as np


@dataclass
class CheckResult:
    """Result from a data quality check.

    Attributes:
        check: Name of the check that was run
        status: "passed" | "failed" | "warning"
        message: Human-readable description of the result
        details: Optional dictionary with metric values and context
    """

    check: str
    status: str  # "passed" | "failed" | "warning"
    message: str
    details: dict | None = None


def _safe_isnan(data: np.ndarray) -> np.ndarray:
    """Return a boolean mask of NaN values, safe for all dtypes.

    Returns an all-False mask for integer/boolean dtypes where NaN is impossible.
    """
    if np.issubdtype(data.dtype, np.floating) or np.issubdtype(data.dtype, np.complexfloating):
        return np.isnan(data)
    return np.zeros(data.shape, dtype=bool)


def check_entropy(data: np.ndarray, min_entropy: float) -> CheckResult:
    """Check Shannon entropy of data distribution.

    Measures information content by computing entropy on value distribution.
    Low entropy indicates degenerate outputs (e.g., model always predicts same value).

    Args:
        data: Input array (any numeric dtype)
        min_entropy: Minimum acceptable entropy (in bits)

    Returns:
        CheckResult with status "passed", "failed", or "warning"

    Examples:
        >>> data = np.array([1, 2, 3, 4, 5])
        >>> result = check_entropy(data, min_entropy=1.0)
        >>> result.status
        'passed'
    """
    # Handle empty arrays
    if data.size == 0:
        return CheckResult(
            check="entropy",
            status="warning",
            message="Cannot compute entropy: array is empty",
            details=None,
        )

    # Remove NaN values
    valid_data = data[~_safe_isnan(data)]

    # Handle all-NaN arrays
    if valid_data.size == 0:
        return CheckResult(
            check="entropy",
            status="warning",
            message="Cannot compute entropy: no valid (non-NaN) values",
            details=None,
        )

    # Compute value counts
    unique, counts = np.unique(valid_data, return_counts=True)

    # Calculate Shannon entropy: H = -sum(p_i * log2(p_i))
    probabilities = counts / counts.sum()
    # Filter out zero probabilities (shouldn't happen, but safe)
    probabilities = probabilities[probabilities > 0]
    entropy = -np.sum(probabilities * np.log2(probabilities))

    # Check threshold
    if entropy >= min_entropy:
        return CheckResult(
            check="entropy",
            status="passed",
            message=f"Entropy check passed: {entropy:.4f} >= {min_entropy:.4f}",
            details={"entropy": float(entropy), "min_entropy": min_entropy},
        )
    else:
        return CheckResult(
            check="entropy",
            status="failed",
            message=f"Entropy check failed: {entropy:.4f} < {min_entropy:.4f}",
            details={"entropy": float(entropy), "min_entropy": min_entropy},
        )


def check_null_ratio(data: np.ndarray, max_null_ratio: float) -> CheckResult:
    """Check ratio of null/NaN values in array.

    Detects excessive missing values which may indicate data pipeline issues.

    Args:
        data: Input array (any dtype)
        max_null_ratio: Maximum acceptable ratio of null values (0.0 to 1.0)

    Returns:
        CheckResult with status "passed", "failed", or "warning"

    Examples:
        >>> data = np.array([1, 2, 3, np.nan, 5])
        >>> result = check_null_ratio(data, max_null_ratio=0.3)
        >>> result.status
        'passed'
    """
    # Handle empty arrays
    if data.size == 0:
        return CheckResult(
            check="null_ratio",
            status="warning",
            message="Cannot compute null ratio: array is empty",
            details=None,
        )

    # Count NaN values
    null_count = _safe_isnan(data).sum()
    total_count = data.size
    null_ratio = float(null_count) / total_count

    # Check threshold (at threshold = passed, above = failed)
    if null_ratio <= max_null_ratio:
        return CheckResult(
            check="null_ratio",
            status="passed",
            message=f"Null ratio check passed: {null_ratio:.4f} <= {max_null_ratio:.4f}",
            details={"null_ratio": null_ratio, "max_null_ratio": max_null_ratio},
        )
    else:
        return CheckResult(
            check="null_ratio",
            status="failed",
            message=f"Null ratio check failed: {null_ratio:.4f} > {max_null_ratio:.4f}",
            details={"null_ratio": null_ratio, "max_null_ratio": max_null_ratio},
        )


def check_vocab_utilization(data: np.ndarray, vocab_size: int, min_utilization: float) -> CheckResult:
    """Check vocabulary utilization for token data.

    Measures how many unique tokens are used relative to vocabulary size.
    Low utilization may indicate model collapse or training issues.

    Args:
        data: Input array of token IDs (integer dtype expected)
        vocab_size: Size of the vocabulary
        min_utilization: Minimum acceptable utilization ratio (0.0 to 1.0)

    Returns:
        CheckResult with status "passed" or "failed"

    Examples:
        >>> data = np.array([1, 2, 3, 1, 2, 3])  # 3 unique values
        >>> result = check_vocab_utilization(data, vocab_size=100, min_utilization=0.01)
        >>> result.status
        'passed'
    """
    # Remove NaN values for unique count
    valid_data = data[~_safe_isnan(data)]

    # Count unique values
    unique_count = len(np.unique(valid_data))
    utilization = float(unique_count) / vocab_size

    # Check threshold (at threshold = passed)
    if utilization >= min_utilization:
        return CheckResult(
            check="vocab_utilization",
            status="passed",
            message=f"Vocab utilization check passed: {utilization:.4f} >= {min_utilization:.4f}",
            details={
                "utilization": utilization,
                "unique_tokens": unique_count,
                "vocab_size": vocab_size,
                "min_utilization": min_utilization,
            },
        )
    else:
        return CheckResult(
            check="vocab_utilization",
            status="failed",
            message=f"Vocab utilization check failed: {utilization:.4f} < {min_utilization:.4f}",
            details={
                "utilization": utilization,
                "unique_tokens": unique_count,
                "vocab_size": vocab_size,
                "min_utilization": min_utilization,
            },
        )


def check_top_k_concentration(data: np.ndarray, k: int, max_concentration: float) -> CheckResult:
    """Check concentration of top-k most frequent values.

    Detects over-reliance on few values which may indicate model issues.
    Concentration = sum(top-k frequencies) / total_count

    Args:
        data: Input array (any numeric dtype)
        k: Number of top values to consider
        max_concentration: Maximum acceptable concentration (0.0 to 1.0)

    Returns:
        CheckResult with status "passed" or "failed"

    Examples:
        >>> data = np.array([1, 2, 3, 4, 5] * 100)  # Uniform
        >>> result = check_top_k_concentration(data, k=2, max_concentration=0.5)
        >>> result.status
        'passed'
    """
    # Remove NaN values
    valid_data = data[~_safe_isnan(data)]

    # Get value counts
    unique, counts = np.unique(valid_data, return_counts=True)
    num_unique = len(unique)

    # Special case: single unique value is always a degenerate failure
    # (data has no diversity at all)
    if num_unique == 1:
        return CheckResult(
            check="top_k_concentration",
            status="failed",
            message=f"Top-{k} concentration check failed: only 1 unique value (degenerate)",
            details={
                "concentration": 1.0,
                "k": k,
                "unique_values": num_unique,
                "max_concentration": max_concentration,
            },
        )

    # When k >= unique values, concentration is always 1.0 by definition
    # If the threshold is lenient (>= 0.99), pass since k is just too large
    # Otherwise, fail since threshold is meaningfully violated
    if k >= num_unique:
        concentration = 1.0
        if max_concentration >= 0.99:
            return CheckResult(
                check="top_k_concentration",
                status="passed",
                message=f"Top-{k} concentration check passed: k >= unique values ({num_unique})",
                details={
                    "concentration": concentration,
                    "k": k,
                    "unique_values": num_unique,
                    "max_concentration": max_concentration,
                },
            )
        else:
            return CheckResult(
                check="top_k_concentration",
                status="failed",
                message=f"Top-{k} concentration check failed: {concentration:.4f} > {max_concentration:.4f}",
                details={
                    "concentration": concentration,
                    "k": k,
                    "unique_values": num_unique,
                    "max_concentration": max_concentration,
                },
            )

    # Get top-k counts (sort descending)
    sorted_indices = np.argsort(-counts)  # Negative for descending
    top_k_counts = counts[sorted_indices[:k]]

    # Calculate concentration
    total_count = counts.sum()
    concentration = float(top_k_counts.sum()) / total_count

    # Check threshold (at threshold = passed)
    if concentration <= max_concentration:
        return CheckResult(
            check="top_k_concentration",
            status="passed",
            message=f"Top-{k} concentration check passed: {concentration:.4f} <= {max_concentration:.4f}",
            details={
                "concentration": concentration,
                "k": k,
                "max_concentration": max_concentration,
            },
        )
    else:
        return CheckResult(
            check="top_k_concentration",
            status="failed",
            message=f"Top-{k} concentration check failed: {concentration:.4f} > {max_concentration:.4f}",
            details={
                "concentration": concentration,
                "k": k,
                "max_concentration": max_concentration,
            },
        )
