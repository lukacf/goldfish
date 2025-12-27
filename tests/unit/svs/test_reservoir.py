"""Unit tests for reservoir sampling.

Tests the ReservoirSampler class which provides memory-efficient sampling
from large arrays using reservoir sampling algorithm.
"""

import numpy as np

from goldfish.svs.checks.reservoir import ReservoirSampler


class TestReservoirSamplerBasics:
    """Test basic functionality of ReservoirSampler."""

    def test_samples_from_small_array_keeps_all(self):
        """When array size < max_samples, should keep all values."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])

        sampler.add(values)
        sample = sampler.get_sample()

        assert len(sample) == 5
        assert set(sample) == {1.0, 2.0, 3.0, 4.0, 5.0}

    def test_samples_from_large_array_limits_to_max(self):
        """When array size > max_samples, should limit to max_samples."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.arange(1000, dtype=np.float64)

        sampler.add(values)
        sample = sampler.get_sample()

        assert len(sample) == 100
        # All sampled values should be from original array
        assert all(v in values for v in sample)

    def test_empty_reservoir_returns_empty_array(self):
        """Before any values added, should return empty array."""
        sampler = ReservoirSampler(max_samples=100)

        sample = sampler.get_sample()

        assert len(sample) == 0
        assert isinstance(sample, np.ndarray)

    def test_count_tracks_total_seen(self):
        """count() should return total values seen, not sample size."""
        sampler = ReservoirSampler(max_samples=50)

        sampler.add(np.arange(30, dtype=np.float64))
        assert sampler.count() == 30

        sampler.add(np.arange(40, dtype=np.float64))
        assert sampler.count() == 70

        sampler.add(np.arange(100, dtype=np.float64))
        assert sampler.count() == 170
        # Sample size should still be limited
        assert len(sampler.get_sample()) == 50

    def test_default_max_samples_is_10000(self):
        """Default max_samples should be 10000."""
        sampler = ReservoirSampler()
        values = np.arange(15000, dtype=np.float64)

        sampler.add(values)
        sample = sampler.get_sample()

        assert len(sample) == 10000
        assert sampler.count() == 15000


class TestReservoirSamplerStatistics:
    """Test statistical properties of sampling."""

    def test_sample_preserves_distribution(self):
        """Sample mean and std should be close to original distribution."""
        np.random.seed(42)
        sampler = ReservoirSampler(max_samples=1000)

        # Create normal distribution
        values = np.random.normal(loc=50.0, scale=10.0, size=10000)
        original_mean = np.mean(values)
        original_std = np.std(values)

        sampler.add(values)
        sample = sampler.get_sample()
        sample_mean = np.mean(sample)
        sample_std = np.std(sample)

        # Mean should be within 5% of original
        assert abs(sample_mean - original_mean) / original_mean < 0.05
        # Std should be within 10% of original
        assert abs(sample_std - original_std) / original_std < 0.10

    def test_sample_covers_full_range(self):
        """Sample should include values from across the full range."""
        np.random.seed(42)
        sampler = ReservoirSampler(max_samples=500)

        values = np.arange(10000, dtype=np.float64)
        original_min = np.min(values)
        original_max = np.max(values)

        sampler.add(values)
        sample = sampler.get_sample()
        sample_min = np.min(sample)
        sample_max = np.max(sample)

        # Sample should cover at least 80% of the range
        assert sample_min < original_max * 0.1
        assert sample_max > original_max * 0.9

    def test_multiple_adds_combine_correctly(self):
        """Multiple add() calls should sample from all batches fairly."""
        np.random.seed(42)
        sampler = ReservoirSampler(max_samples=300)

        # Add three batches with distinct ranges
        batch1 = np.full(1000, 1.0)  # All 1s
        batch2 = np.full(1000, 2.0)  # All 2s
        batch3 = np.full(1000, 3.0)  # All 3s

        sampler.add(batch1)
        sampler.add(batch2)
        sampler.add(batch3)

        sample = sampler.get_sample()
        assert len(sample) == 300

        # Each batch should be represented (roughly 100 each)
        # Allow for some variance due to randomness
        count_1s = np.sum(sample == 1.0)
        count_2s = np.sum(sample == 2.0)
        count_3s = np.sum(sample == 3.0)

        # Each should have between 50 and 150 (within 50% of expected 100)
        assert 50 <= count_1s <= 150
        assert 50 <= count_2s <= 150
        assert 50 <= count_3s <= 150


class TestReservoirSamplerEdgeCases:
    """Test edge cases and error conditions."""

    def test_nan_values_handled(self):
        """NaN values should be preserved in sample."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.array([1.0, 2.0, np.nan, 4.0, np.nan, 6.0])

        sampler.add(values)
        sample = sampler.get_sample()

        assert len(sample) == 6
        assert np.sum(np.isnan(sample)) == 2

    def test_inf_values_handled(self):
        """Inf values should be preserved in sample."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.array([1.0, np.inf, -np.inf, 4.0, 5.0])

        sampler.add(values)
        sample = sampler.get_sample()

        assert len(sample) == 5
        assert np.sum(np.isinf(sample)) == 2
        assert np.any(sample == np.inf)
        assert np.any(sample == -np.inf)

    def test_single_value_array(self):
        """Single value array should work correctly."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.array([42.0])

        sampler.add(values)
        sample = sampler.get_sample()

        assert len(sample) == 1
        assert sample[0] == 42.0

    def test_all_same_values(self):
        """Array with all same values should be handled."""
        sampler = ReservoirSampler(max_samples=50)
        values = np.full(1000, 7.77)

        sampler.add(values)
        sample = sampler.get_sample()

        assert len(sample) == 50
        assert np.all(sample == 7.77)

    def test_very_large_max_samples(self):
        """Very large max_samples should work without memory issues."""
        sampler = ReservoirSampler(max_samples=1_000_000)
        values = np.arange(500, dtype=np.float64)

        sampler.add(values)
        sample = sampler.get_sample()

        # Should keep all since less than max
        assert len(sample) == 500

    def test_zero_length_array_ignored(self):
        """Empty array should be handled gracefully."""
        sampler = ReservoirSampler(max_samples=100)
        empty = np.array([], dtype=np.float64)

        sampler.add(empty)
        sample = sampler.get_sample()

        assert len(sample) == 0
        assert sampler.count() == 0


class TestReservoirSamplerDtypes:
    """Test handling of different array dtypes."""

    def test_float32_preserved(self):
        """float32 dtype should be preserved."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32)

        sampler.add(values)
        sample = sampler.get_sample()

        assert sample.dtype == np.float32

    def test_float64_preserved(self):
        """float64 dtype should be preserved."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float64)

        sampler.add(values)
        sample = sampler.get_sample()

        assert sample.dtype == np.float64

    def test_int_arrays_work(self):
        """Integer arrays should be handled (converted to float)."""
        sampler = ReservoirSampler(max_samples=100)
        values = np.array([1, 2, 3, 4, 5], dtype=np.int64)

        sampler.add(values)
        sample = sampler.get_sample()

        # Should be converted to float for consistency
        assert len(sample) == 5
        assert np.all(np.isin(sample, [1.0, 2.0, 3.0, 4.0, 5.0]))

    def test_mixed_dtypes_across_adds(self):
        """Multiple adds with different dtypes should be handled."""
        sampler = ReservoirSampler(max_samples=100)

        values1 = np.array([1.0, 2.0], dtype=np.float32)
        values2 = np.array([3.0, 4.0], dtype=np.float64)

        sampler.add(values1)
        sampler.add(values2)

        sample = sampler.get_sample()
        assert len(sample) == 4
        # Result dtype should be consistent (likely float64, the wider type)
        assert sample.dtype in (np.float32, np.float64)
