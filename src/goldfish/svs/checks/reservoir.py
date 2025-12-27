"""Reservoir sampling for memory-efficient statistical analysis.

Implements the reservoir sampling algorithm to maintain a fixed-size sample
from a stream of values, ensuring each value has equal probability of being
selected regardless of when it arrives.
"""

from __future__ import annotations

import numpy as np


class ReservoirSampler:
    """Memory-efficient sampling from large arrays using reservoir sampling.

    The reservoir sampling algorithm maintains a fixed-size sample from a
    potentially infinite stream of values. Each value has equal probability
    of being in the final sample, regardless of when it was seen.

    This is useful for computing statistics on very large datasets without
    loading all values into memory at once.

    Attributes:
        max_samples: Maximum number of samples to retain in the reservoir.
        _reservoir: Internal array storing the current sample.
        _count: Total number of values seen across all add() calls.
        _dtype: Data type of the values (preserved from first non-empty add).
    """

    def __init__(self, max_samples: int = 10000) -> None:
        """Initialize reservoir sampler.

        Args:
            max_samples: Maximum number of samples to keep. Default 10000.
        """
        self.max_samples = max_samples
        self._reservoir: np.ndarray | None = None
        self._count = 0
        self._dtype: np.dtype | None = None

    def add(self, values: np.ndarray) -> None:
        """Add values to reservoir using reservoir sampling algorithm.

        Implements Algorithm R from "Random Sampling with a Reservoir"
        (Vitter, 1985). Each value has equal probability max_samples/n
        of being in the final sample, where n is the total count.

        Args:
            values: Array of values to add to the reservoir.
        """
        if len(values) == 0:
            return

        # Convert to float if needed, preserving float32/float64
        if np.issubdtype(values.dtype, np.integer):
            values = values.astype(np.float64)

        # Initialize reservoir on first non-empty add
        if self._reservoir is None:
            self._dtype = values.dtype
            self._reservoir = np.array([], dtype=self._dtype)

        # Convert values to reservoir dtype for consistency
        if values.dtype != self._dtype:
            # Promote to wider dtype if necessary
            if values.dtype == np.float64 and self._dtype == np.float32:
                self._dtype = np.float64
                self._reservoir = self._reservoir.astype(np.float64)
            values = values.astype(self._dtype)

        # If reservoir not full yet, just append
        current_size = len(self._reservoir)
        if current_size < self.max_samples:
            space_left = self.max_samples - current_size
            to_add = min(space_left, len(values))
            self._reservoir = np.concatenate([self._reservoir, values[:to_add]])
            self._count += to_add

            # If we still have values left, process them with reservoir algorithm
            if to_add < len(values):
                remaining_values = values[to_add:]
                self._add_with_replacement(remaining_values)
            return

        # Reservoir is full, use replacement algorithm
        self._add_with_replacement(values)

    def _add_with_replacement(self, values: np.ndarray) -> None:
        """Add values using reservoir sampling replacement algorithm.

        For each value, randomly decide if it should be in the reservoir
        For each value, randomly decide if it should be in the reservoir
        of being in the final sample, where n is the total count.
        of being in the final sample, where n is the total count.

        Args:
            values: Array of values to potentially add to reservoir.
        """
        if self._reservoir is None:
            # Should not happen as add() ensures initialization
            return

        for _i, value in enumerate(values):
            # For each new value, decide if it should be in the reservoir
            # Probability is max_samples / (count + 1)
            self._count += 1
            # Random index in range [0, count)
            j = np.random.randint(0, self._count)
            # If j < max_samples, replace that position
            if j < self.max_samples:
                self._reservoir[j] = value

    def get_sample(self) -> np.ndarray:
        """Return current sample from reservoir.

        Returns:
            Array containing current reservoir sample. Empty array if no
            values have been added yet. The array is a copy, so modifying
            it won't affect the reservoir.
        """
        if self._reservoir is None:
            return np.array([])
        return self._reservoir.copy()

    def count(self) -> int:
        """Return total number of values seen.

        Returns:
            Total count of all values added via add() calls, not just the
            current sample size.
        """
        return self._count
