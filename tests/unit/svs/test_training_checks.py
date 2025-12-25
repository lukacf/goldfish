"""Unit tests for SVS training checks - During-run monitoring helpers.

These tests verify the training_checks module which provides lightweight
user-invoked helpers for detecting training issues during stage execution.

All checks are mechanistic and designed for <1ms latency.
"""

import math

from goldfish.svs.checks.training_checks import (
    check_gradient_explosion,
    check_loss_divergence,
    check_metric_health,
)


class TestCheckMetricHealth:
    """Tests for check_metric_health - NaN/Inf detection."""

    def test_check_metric_health_detects_nan_values(self):
        """check_metric_health detects NaN values and returns failed status."""
        result = check_metric_health("train_loss", float("nan"), step=10)

        assert result.check == "metric_health"
        assert result.status == "failed"
        assert "nan" in result.message.lower()
        assert result.details is not None
        assert result.details["name"] == "train_loss"
        assert result.details["step"] == 10
        assert math.isnan(result.details["value"])

    def test_check_metric_health_detects_positive_inf(self):
        """check_metric_health detects positive infinity and returns failed status."""
        result = check_metric_health("learning_rate", float("inf"), step=5)

        assert result.check == "metric_health"
        assert result.status == "failed"
        assert "inf" in result.message.lower()
        assert result.details is not None
        assert result.details["name"] == "learning_rate"
        assert math.isinf(result.details["value"])

    def test_check_metric_health_detects_negative_inf(self):
        """check_metric_health detects negative infinity and returns failed status."""
        result = check_metric_health("loss", float("-inf"), step=100)

        assert result.check == "metric_health"
        assert result.status == "failed"
        assert "inf" in result.message.lower()
        assert result.details is not None
        assert math.isinf(result.details["value"])

    def test_check_metric_health_passes_for_normal_values(self):
        """check_metric_health passes for normal finite values."""
        result = check_metric_health("accuracy", 0.95, step=50)

        assert result.check == "metric_health"
        assert result.status == "passed"
        assert "healthy" in result.message.lower()
        assert result.details is not None
        assert result.details["value"] == 0.95
        assert result.details["name"] == "accuracy"
        assert result.details["step"] == 50

    def test_check_metric_health_passes_for_zero(self):
        """check_metric_health passes for zero value."""
        result = check_metric_health("loss", 0.0, step=1)

        assert result.check == "metric_health"
        assert result.status == "passed"
        assert result.details["value"] == 0.0

    def test_check_metric_health_passes_for_negative_values(self):
        """check_metric_health passes for negative finite values."""
        result = check_metric_health("reward", -10.5, step=20)

        assert result.check == "metric_health"
        assert result.status == "passed"
        assert result.details["value"] == -10.5

    def test_check_metric_health_passes_for_very_large_finite_values(self):
        """check_metric_health passes for very large but finite values."""
        result = check_metric_health("count", 1e100, step=1)

        assert result.check == "metric_health"
        assert result.status == "passed"
        assert result.details["value"] == 1e100


class TestCheckLossDivergence:
    """Tests for check_loss_divergence - Loss explosion detection."""

    def test_check_loss_divergence_detects_10x_spike(self):
        """check_loss_divergence detects 10x spike in loss within window."""
        # Stable loss then 10x explosion
        history = [1.0, 1.1, 1.05, 1.2, 10.5]

        result = check_loss_divergence(history, window=4, threshold=5.0)

        assert result.check == "loss_divergence"
        assert result.status == "failed"
        assert "divergence" in result.message.lower() or "spike" in result.message.lower()
        assert result.details is not None
        assert "max_ratio" in result.details or "divergence_ratio" in result.details

    def test_check_loss_divergence_requires_minimum_window_size(self):
        """check_loss_divergence requires at least 2 values in history."""
        history = [1.0]

        result = check_loss_divergence(history, window=3, threshold=5.0)

        assert result.check == "loss_divergence"
        assert result.status == "warning"
        assert "insufficient" in result.message.lower() or "minimum" in result.message.lower()

    def test_check_loss_divergence_handles_empty_history(self):
        """check_loss_divergence handles empty history gracefully."""
        result = check_loss_divergence([], window=5, threshold=5.0)

        assert result.check == "loss_divergence"
        assert result.status == "warning"
        assert "empty" in result.message.lower() or "insufficient" in result.message.lower()

    def test_check_loss_divergence_passes_for_stable_loss(self):
        """check_loss_divergence passes when loss remains stable."""
        history = [1.0, 1.1, 1.05, 0.95, 1.02, 0.98]

        result = check_loss_divergence(history, window=5, threshold=3.0)

        assert result.check == "loss_divergence"
        assert result.status == "passed"
        assert "stable" in result.message.lower() or "healthy" in result.message.lower()

    def test_check_loss_divergence_passes_for_gradual_increase(self):
        """check_loss_divergence passes for gradual increase below threshold."""
        history = [1.0, 1.5, 2.0, 2.5]

        result = check_loss_divergence(history, window=4, threshold=5.0)

        assert result.check == "loss_divergence"
        assert result.status == "passed"

    def test_check_loss_divergence_uses_window_parameter(self):
        """check_loss_divergence only considers values within specified window."""
        # Old spike (outside window) followed by stable values
        history = [1.0, 20.0, 1.1, 1.0, 1.05, 1.02]

        result = check_loss_divergence(history, window=4, threshold=3.0)

        # Should pass because spike is outside the 4-value window
        assert result.check == "loss_divergence"
        assert result.status == "passed"

    def test_check_loss_divergence_handles_decreasing_loss(self):
        """check_loss_divergence passes for decreasing loss (normal training)."""
        history = [10.0, 5.0, 2.5, 1.25, 0.6]

        result = check_loss_divergence(history, window=5, threshold=3.0)

        assert result.check == "loss_divergence"
        assert result.status == "passed"

    def test_check_loss_divergence_detects_spike_from_low_baseline(self):
        """check_loss_divergence detects spike even from very low baseline."""
        history = [0.001, 0.0015, 0.0012, 0.01]

        result = check_loss_divergence(history, window=4, threshold=5.0)

        assert result.check == "loss_divergence"
        assert result.status == "failed"


class TestCheckGradientExplosion:
    """Tests for check_gradient_explosion - Gradient norm monitoring."""

    def test_check_gradient_explosion_detects_large_grad_norms(self):
        """check_gradient_explosion detects gradient norms exceeding threshold."""
        result = check_gradient_explosion(grad_norm=1000.0, threshold=100.0)

        assert result.check == "gradient_explosion"
        assert result.status == "failed"
        assert "explosion" in result.message.lower() or "exceeded" in result.message.lower()
        assert result.details is not None
        assert result.details["grad_norm"] == 1000.0
        assert result.details["threshold"] == 100.0

    def test_check_gradient_explosion_passes_for_normal_grad_norms(self):
        """check_gradient_explosion passes for gradient norms below threshold."""
        result = check_gradient_explosion(grad_norm=5.0, threshold=100.0)

        assert result.check == "gradient_explosion"
        assert result.status == "passed"
        assert "healthy" in result.message.lower() or "normal" in result.message.lower()
        assert result.details["grad_norm"] == 5.0

    def test_check_gradient_explosion_passes_at_threshold(self):
        """check_gradient_explosion passes when grad_norm equals threshold."""
        result = check_gradient_explosion(grad_norm=100.0, threshold=100.0)

        assert result.check == "gradient_explosion"
        assert result.status == "passed"

    def test_check_gradient_explosion_handles_zero_grad_norm(self):
        """check_gradient_explosion handles zero gradient norm (may indicate issue)."""
        result = check_gradient_explosion(grad_norm=0.0, threshold=100.0)

        assert result.check == "gradient_explosion"
        # Could be passed (no explosion) or warning (suspicious zero grad)
        assert result.status in ["passed", "warning"]

    def test_check_gradient_explosion_detects_nan_grad_norm(self):
        """check_gradient_explosion detects NaN gradient norm."""
        result = check_gradient_explosion(grad_norm=float("nan"), threshold=100.0)

        assert result.check == "gradient_explosion"
        assert result.status == "failed"
        assert "nan" in result.message.lower()

    def test_check_gradient_explosion_detects_inf_grad_norm(self):
        """check_gradient_explosion detects infinite gradient norm."""
        result = check_gradient_explosion(grad_norm=float("inf"), threshold=100.0)

        assert result.check == "gradient_explosion"
        assert result.status == "failed"
        assert "inf" in result.message.lower() or "infinite" in result.message.lower()

    def test_check_gradient_explosion_passes_for_very_small_grad_norms(self):
        """check_gradient_explosion passes for very small but non-zero gradients."""
        result = check_gradient_explosion(grad_norm=1e-10, threshold=100.0)

        assert result.check == "gradient_explosion"
        assert result.status == "passed"
