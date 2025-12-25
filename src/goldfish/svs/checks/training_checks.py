"""SVS Training Checks - During-run monitoring helpers.

These are lightweight user-invoked helpers for detecting training issues.
All checks are mechanistic and designed for <1ms latency.

Checks:
- check_metric_health: NaN/Inf detection for metrics
- check_loss_divergence: Loss explosion detection over history window
- check_gradient_explosion: Gradient norm monitoring

Usage:
    from goldfish.svs.checks.training_checks import (
        check_metric_health,
        check_loss_divergence,
        check_gradient_explosion,
    )

    # Per-step: cheap NaN/Inf check
    if result := check_metric_health("loss", loss, step):
        if result.status == "failed":
            log_metric("svs_alert", 1, step)

    # Checkpoint: expensive checks
    if step % 1000 == 0:
        if result := check_loss_divergence(loss_history):
            if result.status == "failed":
                log_metric("svs_divergence", 1, step)
"""

from __future__ import annotations

import math

from goldfish.svs.checks.output_checks import CheckResult


def check_metric_health(name: str, value: float, step: int) -> CheckResult:
    """Check if a metric value is healthy (not NaN or Inf).

    Lightweight NaN/Inf check designed for per-step monitoring (<1ms).

    Args:
        name: Metric name (e.g., "train_loss", "accuracy")
        value: Metric value to check
        step: Training step number

    Returns:
        CheckResult with status "passed" or "failed"

    Examples:
        >>> result = check_metric_health("loss", 0.5, step=10)
        >>> result.status
        'passed'

        >>> result = check_metric_health("loss", float("nan"), step=10)
        >>> result.status
        'failed'
    """
    details = {"name": name, "value": value, "step": step}

    # Check for NaN
    if math.isnan(value):
        return CheckResult(
            check="metric_health",
            status="failed",
            message=f"NaN detected in metric '{name}' at step {step}",
            details=details,
        )

    # Check for Inf (positive or negative)
    if math.isinf(value):
        return CheckResult(
            check="metric_health",
            status="failed",
            message=f"Inf detected in metric '{name}' at step {step}",
            details=details,
        )

    # Value is healthy
    return CheckResult(
        check="metric_health",
        status="passed",
        message=f"Metric '{name}' is healthy at step {step}",
        details=details,
    )


def check_loss_divergence(
    history: list[float],
    window: int = 100,
    threshold: float = 10.0,
) -> CheckResult:
    """Check for loss divergence (explosion) within a window.

    Detects sudden spikes (increases) in loss by comparing recent values
    to earlier values. A spike is when recent values are much higher than
    earlier values. Decreasing loss (normal training convergence) is healthy.

    Args:
        history: List of loss values over training
        window: Number of recent values to consider
        threshold: Max allowed ratio of last value to minimum earlier value

    Returns:
        CheckResult with status "passed", "failed", or "warning"

    Examples:
        >>> history = [1.0, 1.1, 1.05, 0.95, 1.02]
        >>> result = check_loss_divergence(history, window=5, threshold=3.0)
        >>> result.status
        'passed'

        >>> history = [1.0, 1.1, 1.05, 10.0]  # Spike
        >>> result = check_loss_divergence(history, window=4, threshold=5.0)
        >>> result.status
        'failed'
    """
    # Handle empty history
    if not history:
        return CheckResult(
            check="loss_divergence",
            status="warning",
            message="Insufficient data: empty loss history",
            details={"history_length": 0, "window": window},
        )

    # Need at least 2 values for meaningful comparison
    if len(history) < 2:
        return CheckResult(
            check="loss_divergence",
            status="warning",
            message="Insufficient data: need minimum 2 values for divergence check",
            details={"history_length": len(history), "window": window},
        )

    # Get the window of recent values
    window_values = history[-window:] if len(history) >= window else history

    # Detect SPIKES (increases), not just wide ranges
    # Compare last value to the minimum of earlier values
    # Spike = last value >> earlier values
    last_val = window_values[-1]
    earlier_values = window_values[:-1]
    min_earlier = min(earlier_values)
    epsilon = 1e-8

    # Calculate spike ratio: how much higher is the last value compared to earlier min?
    # This correctly identifies:
    # - [1.0, 1.0, 10.0] -> spike (last >> earlier min)
    # - [10.0, 5.0, 1.0] -> healthy (last < earlier min, ratio < 1)
    if min_earlier <= 0:
        # Handle edge case with non-positive earlier values
        ratio = last_val / (abs(min_earlier) + epsilon)
    else:
        ratio = last_val / (min_earlier + epsilon)

    details = {
        "window": window,
        "threshold": threshold,
        "max_ratio": float(ratio),
        "divergence_ratio": float(ratio),
        "window_size": len(window_values),
        "last_value": float(last_val),
        "min_earlier_value": float(min_earlier),
    }

    # Check if ratio exceeds threshold (spike detected)
    if ratio > threshold:
        return CheckResult(
            check="loss_divergence",
            status="failed",
            message=f"Loss divergence detected: spike ratio {ratio:.2f} exceeds threshold {threshold:.2f}",
            details=details,
        )

    return CheckResult(
        check="loss_divergence",
        status="passed",
        message=f"Loss is stable: ratio {ratio:.2f} within threshold {threshold:.2f}",
        details=details,
    )


def check_gradient_explosion(
    grad_norm: float,
    threshold: float = 100.0,
) -> CheckResult:
    """Check if gradient norm indicates explosion.

    Monitors gradient norm to detect exploding gradients.

    Args:
        grad_norm: Current gradient norm value
        threshold: Maximum acceptable gradient norm

    Returns:
        CheckResult with status "passed", "failed", or "warning"

    Examples:
        >>> result = check_gradient_explosion(grad_norm=5.0, threshold=100.0)
        >>> result.status
        'passed'

        >>> result = check_gradient_explosion(grad_norm=1000.0, threshold=100.0)
        >>> result.status
        'failed'
    """
    details = {"grad_norm": grad_norm, "threshold": threshold}

    # Check for NaN
    if math.isnan(grad_norm):
        return CheckResult(
            check="gradient_explosion",
            status="failed",
            message="NaN gradient norm detected",
            details=details,
        )

    # Check for Inf
    if math.isinf(grad_norm):
        return CheckResult(
            check="gradient_explosion",
            status="failed",
            message="Infinite gradient norm detected",
            details=details,
        )

    # Check if exceeds threshold (at threshold is OK)
    if grad_norm > threshold:
        return CheckResult(
            check="gradient_explosion",
            status="failed",
            message=f"Gradient explosion detected: norm {grad_norm:.2f} exceeded threshold {threshold:.2f}",
            details=details,
        )

    # Zero gradient could be suspicious but not necessarily an explosion
    if grad_norm == 0.0:
        return CheckResult(
            check="gradient_explosion",
            status="passed",
            message="Gradient norm is zero (may indicate vanishing gradients)",
            details=details,
        )

    return CheckResult(
        check="gradient_explosion",
        status="passed",
        message=f"Gradient norm {grad_norm:.2f} is healthy (below threshold {threshold:.2f})",
        details=details,
    )
