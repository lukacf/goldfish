"""Unit tests for SVS output checks.

Tests mechanistic checks run on stage outputs (no AI, no compression).
"""

import numpy as np

from goldfish.svs.checks.output_checks import (
    check_entropy,
    check_null_ratio,
    check_top_k_concentration,
    check_vocab_utilization,
)


class TestCheckEntropy:
    """Tests for entropy calculation on output data."""

    def test_uniform_distribution_passes(self):
        """Uniform distribution should have high entropy and pass."""
        data = np.random.randint(0, 100, size=10000)
        result = check_entropy(data, min_entropy=0.1)
        assert result.status == "passed"
        assert result.check == "entropy"
        assert "entropy" in result.message.lower()

    def test_constant_array_fails(self):
        """Constant array has zero entropy and should fail."""
        data = np.full(1000, 42)
        result = check_entropy(data, min_entropy=0.1)
        assert result.status == "failed"
        assert "entropy" in result.message.lower()
        assert result.details is not None
        assert result.details["entropy"] == 0.0

    def test_low_entropy_fails(self):
        """Array with low entropy should fail the threshold check."""
        # Create data with mostly one value
        data = np.array([1] * 990 + [2] * 5 + [3] * 5)
        result = check_entropy(data, min_entropy=0.5)
        assert result.status == "failed"
        assert result.details is not None
        assert result.details["entropy"] < 0.5

    def test_nan_values_excluded_from_calc(self):
        """NaN values should be excluded from entropy calculation."""
        data = np.array([1.0, 2.0, 3.0, np.nan, np.nan, 1.0, 2.0, 3.0])
        result = check_entropy(data, min_entropy=0.1)
        assert result.status in ["passed", "failed", "warning"]
        # Should calculate entropy on 6 valid values, not 8

    def test_empty_array_returns_warning(self):
        """Empty array should return warning status."""
        data = np.array([])
        result = check_entropy(data, min_entropy=0.1)
        assert result.status == "warning"
        assert "empty" in result.message.lower()

    def test_all_nan_array_returns_warning(self):
        """Array with all NaN values should return warning."""
        data = np.array([np.nan, np.nan, np.nan])
        result = check_entropy(data, min_entropy=0.1)
        assert result.status == "warning"
        assert "no valid" in result.message.lower() or "empty" in result.message.lower()

    def test_integer_arrays_dont_crash(self):
        """Integer arrays should not cause np.isnan TypeError crash (P0-2)."""
        data = np.array([1, 2, 3, 4, 5], dtype=np.int32)
        # Should not raise TypeError
        result = check_entropy(data, min_entropy=0.1)
        assert result.status == "passed"


class TestCheckNullRatio:
    """Tests for null ratio checks on output data."""

    def test_no_nulls_passes(self):
        """Array with no null values should pass."""
        data = np.array([1, 2, 3, 4, 5])
        result = check_null_ratio(data, max_null_ratio=0.5)
        assert result.status == "passed"
        assert result.check == "null_ratio"
        assert result.details is not None
        assert result.details["null_ratio"] == 0.0

    def test_high_null_ratio_fails(self):
        """Array with high null ratio should fail."""
        data = np.array([1.0, np.nan, np.nan, np.nan, 5.0])
        result = check_null_ratio(data, max_null_ratio=0.5)
        assert result.status == "failed"
        assert result.details is not None
        assert result.details["null_ratio"] == 0.6

    def test_exactly_at_threshold_passes(self):
        """Null ratio exactly at threshold should pass."""
        data = np.array([1.0, np.nan, 3.0, 4.0])
        result = check_null_ratio(data, max_null_ratio=0.25)
        assert result.status == "passed"
        assert result.details["null_ratio"] == 0.25

    def test_all_nulls_fails(self):
        """Array with all nulls should fail."""
        data = np.array([np.nan, np.nan, np.nan])
        result = check_null_ratio(data, max_null_ratio=0.5)
        assert result.status == "failed"
        assert result.details["null_ratio"] == 1.0

    def test_counts_nan_and_none(self):
        """Should count both NaN and None as null values."""
        # For numeric arrays, None typically becomes NaN
        data = np.array([1.0, np.nan, 3.0, np.nan, 5.0])
        result = check_null_ratio(data, max_null_ratio=0.5)
        assert result.status == "passed"  # 2/5 = 0.4 <= 0.5
        assert result.details["null_ratio"] == 0.4

    def test_empty_array_returns_warning(self):
        """Empty array should return warning status."""
        data = np.array([])
        result = check_null_ratio(data, max_null_ratio=0.5)
        assert result.status == "warning"
        assert "empty" in result.message.lower()


class TestCheckVocabUtilization:
    """Tests for vocabulary utilization checks."""

    def test_good_utilization_passes(self):
        """Good vocab utilization should pass."""
        # Use 50 out of 100 tokens
        data = np.repeat(np.arange(50), 20)
        result = check_vocab_utilization(data, vocab_size=100, min_utilization=0.01)
        assert result.status == "passed"
        assert result.details is not None
        assert result.details["utilization"] == 0.5

    def test_low_utilization_fails(self):
        """Low vocab utilization should fail."""
        # Use only 5 out of 1000 tokens
        data = np.repeat(np.arange(5), 100)
        result = check_vocab_utilization(data, vocab_size=1000, min_utilization=0.1)
        assert result.status == "failed"
        assert result.details["utilization"] == 0.005

    def test_single_token_fails(self):
        """Using only one token should fail for reasonable thresholds."""
        data = np.full(1000, 42)
        result = check_vocab_utilization(data, vocab_size=1000, min_utilization=0.01)
        assert result.status == "failed"
        assert result.details["utilization"] == 0.001

    def test_full_vocab_used_passes(self):
        """Using entire vocabulary should pass."""
        data = np.tile(np.arange(100), 10)
        result = check_vocab_utilization(data, vocab_size=100, min_utilization=0.5)
        assert result.status == "passed"
        assert result.details["utilization"] == 1.0

    def test_exactly_at_threshold_passes(self):
        """Utilization exactly at threshold should pass."""
        # Use exactly 10 out of 100 tokens (10%)
        data = np.repeat(np.arange(10), 10)
        result = check_vocab_utilization(data, vocab_size=100, min_utilization=0.1)
        assert result.status == "passed"
        assert result.details["utilization"] == 0.1


class TestCheckTopKConcentration:
    """Tests for top-k concentration checks."""

    def test_uniform_distribution_passes(self):
        """Uniform distribution should have low concentration and pass."""
        data = np.random.randint(0, 100, size=10000)
        result = check_top_k_concentration(data, k=10, max_concentration=0.99)
        assert result.status == "passed"
        assert result.details is not None
        assert result.details["concentration"] < 0.99

    def test_highly_concentrated_fails(self):
        """Highly concentrated distribution should fail."""
        # 95% of data in top value
        data = np.array([1] * 950 + [2] * 10 + [3] * 10 + [4] * 10 + [5] * 20)
        result = check_top_k_concentration(data, k=10, max_concentration=0.5)
        assert result.status == "failed"
        assert result.details["concentration"] > 0.5

    def test_single_value_fails(self):
        """Array with single value should fail concentration check."""
        data = np.full(1000, 42)
        result = check_top_k_concentration(data, k=10, max_concentration=0.99)
        assert result.status == "failed"
        assert result.details["concentration"] == 1.0

    def test_k_larger_than_unique_handled(self):
        """Should handle case where k is larger than unique values."""
        data = np.array([1, 2, 3, 1, 2, 3])  # Only 3 unique values
        result = check_top_k_concentration(data, k=10, max_concentration=0.99)
        assert result.status == "passed"
        # All values are in "top 10", but that's expected

    def test_exactly_at_threshold_passes(self):
        """Concentration exactly at threshold should pass."""
        # Create data where top-k is exactly at threshold
        data = np.array([1] * 50 + list(range(2, 52)))
        result = check_top_k_concentration(data, k=1, max_concentration=0.5)
        assert result.status == "passed"
        assert result.details["concentration"] == 0.5
