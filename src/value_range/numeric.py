"""
Numeric value range implementations.

This module provides numeric value range classes for deterministic data generation.
"""

from abc import abstractmethod
from typing import Optional, Union
from .value_range import ValueRange, InsertionOrder


class NumericRange(ValueRange):
    """
    Abstract base class for numeric value ranges (integers, longs, floats).

    Provides common frequency and step_size management logic for numeric types.

    Frequency represents how many times each value repeats before moving to the next value.
    - frequency = 1: Each value appears once (all unique)
    - frequency = 2: Each value appears twice
    - Higher frequency = more repetition of each value
    """

    def __init__(self, min_value: Union[int, float], max_value: Union[int, float],
                 frequency: Optional[int] = None, step_size: Optional[Union[int, float]] = None,
                 insertion_order: InsertionOrder = InsertionOrder.ASCENDING):
        """
        Initialize a numeric range.

        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            frequency: How many times each value repeats. Default is 1 (all unique values).
            step_size: Step between consecutive values. If None, computed from frequency or max_count.
            insertion_order: The order in which values are inserted (default: ASCENDING)

        Note: If both frequency and step_size are None, frequency defaults to 1 (all unique).
              The algorithm prioritizes maximum uniqueness (frequency=1) with step_size spanning the range.
        """
        super().__init__(insertion_order=insertion_order)

        if min_value > max_value:
            raise ValueError(f"min_value ({min_value}) cannot be greater than max_value ({max_value})")

        self.min_value = min_value
        self.max_value = max_value
        self._explicit_frequency = frequency if frequency is not None and frequency > 0 else None
        self._explicit_step_size = step_size if step_size is not None and step_size > 0 else None

        self._compute_frequency_and_step()

    @abstractmethod
    def _compute_step_from_frequency_and_max_count(self, frequency: int, max_count: Optional[int]) -> Union[int, float]:
        """
        Compute step size given frequency (repetitions per value) and optional max_count.

        Args:
            frequency: How many times each value repeats
            max_count: Total number of documents (if known)

        Returns:
            Step size between consecutive values
        """
        pass

    @abstractmethod
    def _get_range_size(self) -> Union[int, float]:
        """
        Get the size of the range (max_value - min_value).

        Returns:
            Range size
        """
        pass

    def _compute_frequency_and_step(self):
        """
        Compute frequency and step_size based on explicit values and max_count.

        Semantics:
        - frequency = how many times each value repeats (1 = all unique)
        - step_size = distance between consecutive distinct values

        Priority:
        1. If both frequency and step_size are explicit, use them as-is
        2. If only frequency is explicit, compute step_size from it
        3. If only step_size is explicit, use frequency=1 (all unique within step)
        4. If neither is explicit, use frequency=1 (default: all unique values)
        """
        if self._explicit_frequency is not None and self._explicit_step_size is not None:
            # Both explicitly set - use as-is
            self.frequency = self._explicit_frequency
            self.step_size = self._explicit_step_size
        elif self._explicit_frequency is not None:
            # Only frequency set - compute step_size to span range
            self.frequency = self._explicit_frequency
            self.step_size = self._compute_step_from_frequency_and_max_count(self.frequency, self.max_count)
        elif self._explicit_step_size is not None:
            # Only step_size set - use frequency=1 (each value appears once)
            self.frequency = 1
            self.step_size = self._explicit_step_size
        else:
            # Neither set - default to frequency=1 (all unique), compute step_size
            self.frequency = 1
            self.step_size = self._compute_step_from_frequency_and_max_count(1, self.max_count)

    def set_max_count(self, count: int) -> 'NumericRange':
        """
        Set the maximum number of documents and recompute frequency/step_size if needed.

        Args:
            count: Maximum number of documents

        Returns:
            Self for method chaining
        """
        super().set_max_count(count)
        # Only recompute if neither frequency nor step_size were explicitly set
        if self._explicit_frequency is None and self._explicit_step_size is None:
            self._compute_frequency_and_step()
        return self


class IntegerRange(NumericRange):
    """
    Generates integers within a specified range.

    Maps integers {0..N} deterministically to values in [min_value, max_value].
    Uses frequency (repetitions per value) and step_size to control generation.

    Examples:
        >>> int_range = IntegerRange(0, 100, frequency=2, step_size=10)
        >>> # Each value repeats twice: 0,0,10,10,20,20,...
        >>> int_range.generate(0)   # Returns 0
        >>> int_range.generate(1)   # Returns 0 (second occurrence)
        >>> int_range.generate(2)   # Returns 10
        >>> int_range.generate(3)   # Returns 10 (second occurrence)

        >>> int_range = IntegerRange(0, 100, step_size=10)
        >>> # frequency defaults to 1 (all unique): 0,10,20,...
        >>> int_range.generate(0)   # Returns 0
        >>> int_range.generate(1)   # Returns 10
        >>> int_range.generate(10)  # Returns 100
    """

    def __init__(self, min_value: int = 0, max_value: int = 100,
                 frequency: Optional[int] = None, step_size: Optional[int] = None,
                 insertion_order: InsertionOrder = InsertionOrder.ASCENDING):
        """
        Initialize an integer range.

        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            frequency: How many times each value repeats (default: 1 for all unique)
            step_size: Step between consecutive distinct integer values
            insertion_order: The order in which values are inserted (default: ASCENDING)
        """
        super().__init__(min_value=min_value, max_value=max_value,
                        frequency=frequency, step_size=step_size, insertion_order=insertion_order)
        # Calculate number of distinct values we'll cycle through
        self._num_distinct_values = int(self._get_range_size() / self.step_size) + 1

    def _get_range_size(self) -> int:
        """Get the size of the integer range."""
        return self.max_value - self.min_value

    def _compute_step_from_frequency_and_max_count(self, frequency: int, max_count: Optional[int]) -> int:
        """
        Compute integer step size from frequency and max_count.

        Goal: span the full range with step_size.
        If max_count is known: step_size = range_size / (max_count / frequency)
        Otherwise: step_size = 1 (use all values)
        """
        if max_count is not None and max_count > 0:
            # Number of distinct values we can generate = max_count / frequency
            num_distinct = max(1, max_count // frequency)
            # Step size to span the range
            step = max(1, self._get_range_size() // num_distinct)
            return step
        else:
            # Default: step_size = 1 (use consecutive integers)
            return 1

    def generate(self, index: int) -> int:
        """
        Generate an integer deterministically from index.

        With frequency, each value repeats `frequency` times:
        - indices 0 to (frequency-1) -> first value
        - indices frequency to (2*frequency-1) -> second value
        - etc.

        This method generates values in ascending order based on the index.
        The insertion_order transformation is handled by the allocate() method.
        """
        # Which distinct value are we at? (accounting for repetitions)
        value_index = (index // self.frequency) % self._num_distinct_values
        return self.min_value + (value_index * self.step_size)


class LongRange(NumericRange):
    """
    Generates long integers within a specified range.

    Maps integers {0..N} deterministically to values in [min_value, max_value].
    Uses frequency (repetitions per value) and step_size to control generation.
    In Python 3, int and long are unified, but this class exists for MongoDB compatibility
    where long is stored as a 64-bit integer (BSON Int64).

    Examples:
        >>> long_range = LongRange(0, 1000000, frequency=2, step_size=100000)
        >>> # Each value repeats twice: 0,0,100000,100000,...
        >>> long_range.generate(0)   # Returns 0
        >>> long_range.generate(1)   # Returns 0
        >>> long_range.generate(2)   # Returns 100000

        >>> long_range = LongRange(0, 1000000, step_size=1000)
        >>> # frequency defaults to 1: 0,1000,2000,...
        >>> long_range.generate(0)   # Returns 0
        >>> long_range.generate(1)   # Returns 1000
    """

    def __init__(self, min_value: int = 0, max_value: int = 2**63 - 1,
                 frequency: Optional[int] = None, step_size: Optional[int] = None,
                 insertion_order: InsertionOrder = InsertionOrder.ASCENDING):
        """
        Initialize a long integer range.

        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive, default is max 64-bit signed int)
            frequency: How many times each value repeats (default: 1 for all unique)
            step_size: Step between consecutive distinct long values
            insertion_order: The order in which values are inserted (default: ASCENDING)
        """
        super().__init__(min_value=min_value, max_value=max_value,
                        frequency=frequency, step_size=step_size, insertion_order=insertion_order)
        # Calculate number of distinct values we'll cycle through
        self._num_distinct_values = int(self._get_range_size() / self.step_size) + 1

    def _get_range_size(self) -> int:
        """Get the size of the long range."""
        return self.max_value - self.min_value

    def _compute_step_from_frequency_and_max_count(self, frequency: int, max_count: Optional[int]) -> int:
        """
        Compute long step size from frequency and max_count.

        Goal: span the full range with step_size.
        If max_count is known: step_size = range_size / (max_count / frequency)
        Otherwise: use a reasonable default since long ranges can be huge
        """
        if max_count is not None and max_count > 0:
            # Number of distinct values we can generate = max_count / frequency
            num_distinct = max(1, max_count // frequency)
            # Step size to span the range
            step = max(1, self._get_range_size() // num_distinct)
            return step
        else:
            # Default: For very large ranges, use a reasonable step
            # This avoids step_size=1 which would make huge ranges unwieldy
            range_size = self._get_range_size()
            if range_size > 1000000:
                return max(1, range_size // 10000)  # Default to ~10000 distinct values
            else:
                return 1

    def generate(self, index: int) -> int:
        """
        Generate a long integer deterministically from index.

        With frequency, each value repeats `frequency` times:
        - indices 0 to (frequency-1) -> first value
        - indices frequency to (2*frequency-1) -> second value
        - etc.

        This method generates values in ascending order based on the index.
        The insertion_order transformation is handled by the allocate() method.
        """
        # Which distinct value are we at? (accounting for repetitions)
        value_index = (index // self.frequency) % self._num_distinct_values
        return self.min_value + (value_index * self.step_size)


class FloatRange(NumericRange):
    """
    Generates floating-point numbers within a specified range.

    Maps integers {0..N} deterministically to float values in [min_value, max_value].
    Uses frequency (repetitions per value) and step_size to control generation.

    Examples:
        >>> float_range = FloatRange(0.0, 10.0, frequency=2, step_size=1.0)
        >>> # Each value repeats twice: 0.0,0.0,1.0,1.0,2.0,2.0,...
        >>> float_range.generate(0)   # Returns 0.0
        >>> float_range.generate(1)   # Returns 0.0
        >>> float_range.generate(2)   # Returns 1.0

        >>> float_range = FloatRange(0.0, 10.0, step_size=0.5)
        >>> # frequency defaults to 1: 0.0,0.5,1.0,1.5,...
        >>> float_range.generate(0)   # Returns 0.0
        >>> float_range.generate(1)   # Returns 0.5
        >>> float_range.generate(2)   # Returns 1.0
    """

    def __init__(self, min_value: float = 0.0, max_value: float = 100.0,
                 frequency: Optional[int] = None, step_size: Optional[float] = None,
                 insertion_order: InsertionOrder = InsertionOrder.ASCENDING):
        """
        Initialize a float range.

        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            frequency: How many times each value repeats (default: 1 for all unique)
            step_size: Step between consecutive distinct float values
            insertion_order: The order in which values are inserted (default: ASCENDING)
        """
        super().__init__(min_value=min_value, max_value=max_value,
                        frequency=frequency, step_size=step_size, insertion_order=insertion_order)
        # Calculate number of distinct values we'll cycle through
        if self.step_size > 0:
            self._num_distinct_values = int(self._get_range_size() / self.step_size) + 1
        else:
            self._num_distinct_values = 1

    def _get_range_size(self) -> float:
        """Get the size of the float range."""
        return self.max_value - self.min_value

    def _compute_step_from_frequency_and_max_count(self, frequency: int, max_count: Optional[int]) -> float:
        """
        Compute float step size from frequency and max_count.

        Goal: span the full range with step_size.
        If max_count is known: step_size = range_size / (max_count / frequency)
        Otherwise: use a reasonable default (1000 distinct values)
        """
        if max_count is not None and max_count > 0:
            # Number of distinct values we can generate = max_count / frequency
            num_distinct = max(1, max_count // frequency)
            # Step size to span the range
            if num_distinct > 1:
                step = self._get_range_size() / (num_distinct - 1)
            else:
                step = 0.0
            return step
        else:
            # Default: reasonable number of distinct float values
            # Use 1000 distinct values by default
            range_size = self._get_range_size()
            return range_size / 999.0  # 1000 distinct values (0 to 999)

    def generate(self, index: int) -> float:
        """
        Generate a float deterministically from index.

        With frequency, each value repeats `frequency` times:
        - indices 0 to (frequency-1) -> first value
        - indices frequency to (2*frequency-1) -> second value
        - etc.

        This method generates values in ascending order based on the index.
        The insertion_order transformation is handled by the allocate() method.
        """
        # Which distinct value are we at? (accounting for repetitions)
        value_index = (index // self.frequency) % self._num_distinct_values
        return self.min_value + (value_index * self.step_size)
