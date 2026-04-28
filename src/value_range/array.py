"""
Numeric array value range implementation.

This module provides NumericArrayRange for deterministic array data generation
with controllable selectivity, suitable for multikey index testing.

Key design decisions:
- Selectivity is calculated globally (across all element positions)
- Uses a single global slot shuffle: slot = doc_id * array_size + pos
- frequency=1: bijection guarantees every element value is globally unique
- frequency>1: each value appears exactly N times; rare intra-array
  collisions resolved via rejection with alternate seed
- Only RANDOM insertion_order is currently supported
"""

from typing import Optional, List, Tuple, Any
import random as python_random
import warnings

from .value_range import ValueRange, InsertionOrder
from .utils import feistel_shuffle


class NumericArrayRange(ValueRange):
    """
    Generates arrays of integers with controllable selectivity for multikey index testing.

    Maps (doc_id, position) pairs deterministically to integer values via
    a global slot shuffle. Each slot = doc_id * array_size + pos is shuffled
    through a single feistel permutation, then divided by frequency to
    produce a value_index.

    Selectivity is global: docs_per_value = frequency, where frequency is
    the number of (doc, position) slots each distinct value occupies.

    Global distinct values = total_slots / frequency
    where total_slots = num_docs * array_size

    Constraints:
    - num_distinct >= array_size (otherwise same-array uniqueness is impossible)
    - Only RANDOM insertion_order is currently supported

    Note on inherited methods:
    - get(doc_id) is inherited from ValueRange and returns a list (same as get_array).
      Prefer using get_array(doc_id) and get_element(doc_id, position) for clarity.

    Limitations:
    - num_distinct is capped by the value range: num_distinct <= (max_value - min_value) / step_size + 1
      If total_slots / frequency exceeds the value range, values will cycle (wrap around).
    - Rejection for same-array uniqueness causes ~<5% frequency deviation when
      num_distinct < 300 (e.g., 100k docs with 5000 doc/val → 60 distinct).
    - Only integer element values are supported.

    Examples:
        >>> arr = NumericArrayRange(0, 999, array_size=3)
        >>> arr.set_max_count(100000)
        >>> # 100k docs, 3 elements each, 300k total slots
        >>> # frequency=1, range=1000 → num_distinct = min(1000, 300000) = 1000
        >>> # each value appears 300 times (300k slots / 1000 distinct)
        >>> # docs_per_value ≈ 100 (300 appearances / 3 positions)

        >>> arr = NumericArrayRange(0, 999, array_size=3, frequency=100)
        >>> arr.set_max_count(100000)
        >>> # total_slots=300k, frequency=100 → wanted num_distinct=3000
        >>> # but range only has 1000 values → num_distinct capped at 1000
        >>> # actual frequency = 300k / 1000 = 300 (not 100!)
        >>> # To get frequency=100, use a larger range: NumericArrayRange(0, 2999, ...)

        >>> arr = NumericArrayRange(0, 2999, array_size=3, frequency=100)
        >>> arr.set_max_count(100000)
        >>> # total_slots=300k, frequency=100 → num_distinct=3000
        >>> # range=3000 → num_distinct=3000 ✓, step_size=1
        >>> # each value appears exactly 100 times
    """

    _MAX_RETRIES = 100  # Maximum rejection retries per array position

    def __init__(
        self,
        min_value: int = 0,
        max_value: int = 100,
        array_size: int = 3,
        frequency: Optional[int] = None,
        step_size: Optional[int] = None,
        insertion_order: InsertionOrder = InsertionOrder.RANDOM,
    ):
        """
        Initialize a numeric array range.

        Args:
            min_value: Minimum element value (inclusive)
            max_value: Maximum element value (inclusive)
            array_size: Number of elements per array (default: 3)
            frequency: How many (doc, position) slots each value occupies.
                       Determines selectivity: docs_per_value ≈ frequency.
                       If None, defaults to 1 (all unique).
            step_size: Step between consecutive distinct values.
                       If None, computed from frequency and range.
            insertion_order: Only RANDOM is currently supported (default: RANDOM).
                            Other values raise NotImplementedError.

        Raises:
            ValueError: If min_value > max_value or array_size < 1
            NotImplementedError: If insertion_order is not RANDOM
        """
        super().__init__(insertion_order=insertion_order)

        if insertion_order != InsertionOrder.RANDOM:
            raise NotImplementedError(
                f"Only RANDOM insertion_order is currently supported for NumericArrayRange. "
                f"Got: {insertion_order.value}"
            )

        if min_value > max_value:
            raise ValueError(f"min_value ({min_value}) cannot be greater than max_value ({max_value})")

        if array_size < 1:
            raise ValueError(f"array_size must be >= 1, got {array_size}")

        self.min_value = min_value
        self.max_value = max_value
        self.array_size = array_size
        self._explicit_frequency = frequency if frequency is not None and frequency > 0 else None
        self._explicit_step_size = step_size if step_size is not None and step_size > 0 else None

        # Will be computed when max_count is set
        self._num_distinct = None
        self.step_size = None
        self.frequency = frequency if frequency is not None and frequency > 0 else 1
        self.document_count = 0

        self._compute_params()

    def _compute_params(self):
        """
        Compute num_distinct and step_size from frequency, step_size, and range.

        num_distinct is always capped by the value range:
            max_possible_distinct = (max_value - min_value) / step_size + 1

        If total_slots / frequency exceeds this cap, values will cycle.
        A warning is issued when this happens.

        Priority (same as IntegerRange):
        1. Both frequency and step_size given → use as-is, validate
        2. Only frequency given → compute step_size
        3. Only step_size given → frequency=1, compute num_distinct
        4. Neither given → frequency=1, step_size=1
        """
        range_size = self.max_value - self.min_value
        max_possible_distinct = range_size + 1  # cap by value range

        if self._explicit_frequency is not None and self._explicit_step_size is not None:
            # Both given - use as-is
            self.frequency = self._explicit_frequency
            self.step_size = self._explicit_step_size
            self._num_distinct = min((range_size // self.step_size) + 1, max_possible_distinct)

            # Validate
            if self._num_distinct < self.array_size:
                raise ValueError(
                    f"num_distinct ({self._num_distinct}) must be >= array_size ({self.array_size}). "
                    f"Increase range or decrease step_size."
                )

            # Check consistency with max_count if available
            if self.max_count is not None:
                total_slots = self.max_count * self.array_size
                expected_distinct = total_slots // self.frequency
                if expected_distinct > self._num_distinct:
                    warnings.warn(
                        f"frequency ({self.frequency}) and step_size ({self.step_size}) imply "
                        f"num_distinct={self._num_distinct}, but total_slots/frequency="
                        f"{expected_distinct}. Values will cycle. "
                        f"To avoid cycling, increase the value range or decrease frequency.",
                        UserWarning,
                    )

        elif self._explicit_frequency is not None:
            # Only frequency given
            self.frequency = self._explicit_frequency
            if self.max_count is not None:
                total_slots = self.max_count * self.array_size
                wanted_distinct = total_slots // self.frequency
                self._num_distinct = min(wanted_distinct, max_possible_distinct)

                if wanted_distinct > max_possible_distinct:
                    warnings.warn(
                        f"frequency={self.frequency} requires {wanted_distinct} distinct values, "
                        f"but value range [{ self.min_value}, {self.max_value}] only supports "
                        f"{max_possible_distinct}. Actual frequency will be "
                        f"{total_slots // self._num_distinct} (values will cycle). "
                        f"To get exact frequency={self.frequency}, use a larger value range.",
                        UserWarning,
                    )
            else:
                # No max_count yet, use full range
                self._num_distinct = max_possible_distinct

            # Ensure minimum
            self._num_distinct = max(self.array_size, self._num_distinct)
            if self._num_distinct > max_possible_distinct:
                raise ValueError(
                    f"num_distinct ({self._num_distinct}) must be >= array_size ({self.array_size}), "
                    f"but value range [{self.min_value}, {self.max_value}] only supports "
                    f"{max_possible_distinct}. Increase the value range or decrease array_size."
                )
            self.step_size = max(1, range_size // max(1, self._num_distinct - 1)) if self._num_distinct > 1 else 1

        elif self._explicit_step_size is not None:
            # Only step_size given
            self.frequency = 1
            self.step_size = self._explicit_step_size
            self._num_distinct = min((range_size // self.step_size) + 1, max_possible_distinct)

            if self._num_distinct < self.array_size:
                raise ValueError(
                    f"num_distinct ({self._num_distinct}) must be >= array_size ({self.array_size}). "
                    f"Increase range or decrease step_size."
                )

        else:
            # Neither given - use full range
            self.frequency = 1
            self.step_size = 1
            self._num_distinct = max_possible_distinct

            if self._num_distinct < self.array_size:
                raise ValueError(
                    f"num_distinct ({self._num_distinct}) must be >= array_size ({self.array_size}). "
                    f"Increase range or decrease array_size."
                )

    def set_max_count(self, count: int) -> 'NumericArrayRange':
        """
        Set the total number of documents and recompute parameters.

        Args:
            count: Total number of documents to generate

        Returns:
            Self for method chaining
        """
        super().set_max_count(count)
        self._compute_params()
        return self

    def _generate_value(self, raw_index: int) -> int:
        """
        Map a raw index [0, num_distinct) to an actual value in [min_value, max_value].

        Args:
            raw_index: Index in [0, num_distinct)

        Returns:
            Integer value in [min_value, max_value]
        """
        value_index = raw_index % self._num_distinct
        return self.min_value + (value_index * self.step_size)

    def generate(self, doc_id: int) -> List[int]:
        """
        Generate an array deterministically for a given doc_id.

        Uses a global slot shuffle: each (doc_id, position) pair maps to a
        global slot index, which is shuffled via feistel and then divided by
        frequency to produce a value_index.

        - frequency=1: each value appears exactly once globally (bijection),
          no intra-array collision possible.
        - frequency>1: each value appears exactly `frequency` times globally.
          Rare intra-array collisions are resolved via rejection (re-shuffle
          with a different seed).

        Args:
            doc_id: Document ordinal (0 to num_docs-1)

        Returns:
            List of integers with length = array_size, no duplicates
        """
        if self._num_distinct is None or self.max_count is None:
            raise ValueError("Parameters not computed. Call set_max_count() first.")

        total_slots = self.max_count * self.array_size
        result = []
        seen = set()

        for pos in range(self.array_size):
            global_slot = doc_id * self.array_size + pos
            seed = 0
            while seed < self._MAX_RETRIES:
                shuffled = feistel_shuffle(global_slot, total_slots, seed=seed)
                value_index = shuffled // self.frequency
                value = self._generate_value(value_index)
                if value not in seen:
                    seen.add(value)
                    result.append(value)
                    break
                # Collision within same array (only possible when frequency > 1)
                seed += 1
            else:
                raise RuntimeError(
                    f"Failed to find non-colliding value for doc_id={doc_id}, "
                    f"pos={pos} after {self._MAX_RETRIES} retries. "
                    f"num_distinct={self._num_distinct}, array_size={self.array_size}"
                )

        return result

    def allocate(self, ordinal: int) -> List[int]:
        """
        Generate an array and track document count.

        Use during data insertion. Increments document_count.

        Args:
            ordinal: Document ordinal

        Returns:
            List of integers
        """
        self.document_count += 1
        return self.generate(ordinal)

    def get_array(self, doc_id: int) -> List[int]:
        """
        Get the array for a doc_id without tracking.

        Use for query RHS generation (exact match or range scan).
        Does NOT increment document_count.

        Args:
            doc_id: Document ordinal

        Returns:
            List of integers (same result as allocate for same doc_id)
        """
        return self.generate(doc_id)

    def get_element(self, doc_id: int, position: int) -> int:
        """
        Get a single element value for a (doc_id, position) pair.

        Use for scalar RHS generation (point query, range scan, $in).
        Does NOT increment document_count.

        Args:
            doc_id: Document ordinal
            position: Element position (0 to array_size-1)

        Returns:
            Integer value at that position

        Raises:
            ValueError: If position is out of range
        """
        if position < 0 or position >= self.array_size:
            raise ValueError(f"position must be in [0, {self.array_size}), got {position}")

        array = self.generate(doc_id)
        return array[position]

    def get_percentile(self, percentile: float) -> Tuple[int, int]:
        """
        Get the element value at a specific percentile.

        Based on all distinct element values in ascending order.
        Useful for generating scalar RHS with controlled selectivity.

        Args:
            percentile: 0.0 to 100.0

        Returns:
            Tuple of (value, ordinal)

        Raises:
            ValueError: If num_distinct not computed or percentile out of range

        Example:
            >>> value, _ = arr.get_percentile(90.0)
            >>> db.col.find({"arr": {"$gt": value}})  # ~top 10%
        """
        if self._num_distinct is None:
            raise ValueError("num_distinct not computed. Call set_max_count() first.")

        if percentile < 0.0 or percentile > 100.0:
            raise ValueError(f"Percentile must be between 0.0 and 100.0, got {percentile}")

        ordinal = int((percentile / 100.0) * (self._num_distinct - 1))
        value = self._generate_value(ordinal)
        return (value, ordinal)

    def random_range(
        self,
        min_percentile: float = 0.0,
        max_percentile: float = 100.0,
    ) -> Tuple[int, int]:
        """
        Get a random element value within a percentile range.

        Useful for generating varied scalar RHS values with similar selectivity,
        avoiding cache effects in performance testing.

        Args:
            min_percentile: Lower bound (0.0 to 100.0)
            max_percentile: Upper bound (0.0 to 100.0)

        Returns:
            Tuple of (value, ordinal)

        Raises:
            ValueError: If parameters invalid

        Example:
            >>> value, _ = arr.random_range(40.0, 60.0)
            >>> db.col.find({"arr": {"$gt": value}})  # ~40-60% selectivity
        """
        if self._num_distinct is None:
            raise ValueError("num_distinct not computed. Call set_max_count() first.")

        if min_percentile < 0.0 or min_percentile > 100.0:
            raise ValueError(f"min_percentile must be between 0.0 and 100.0, got {min_percentile}")

        if max_percentile < 0.0 or max_percentile > 100.0:
            raise ValueError(f"max_percentile must be between 0.0 and 100.0, got {max_percentile}")

        if min_percentile > max_percentile:
            raise ValueError(
                f"min_percentile ({min_percentile}) cannot be greater than max_percentile ({max_percentile})"
            )

        min_ordinal = int((min_percentile / 100.0) * (self._num_distinct - 1))
        max_ordinal = int((max_percentile / 100.0) * (self._num_distinct - 1))

        random_ordinal = python_random.randint(min_ordinal, max_ordinal)
        value = self._generate_value(random_ordinal)

        return (value, random_ordinal)

    def random(self) -> int:
        """
        Get a random element value from the distinct values.

        Returns:
            A random element value

        Raises:
            ValueError: If num_distinct not computed
        """
        if self._num_distinct is None:
            raise ValueError("num_distinct not computed. Call set_max_count() first.")

        random_ordinal = python_random.randint(0, self._num_distinct - 1)
        return self._generate_value(random_ordinal)

    def describe(self) -> str:
        """
        Print a human-readable summary of the array range configuration.

        Shows all computed parameters and how they were derived.
        Useful for verifying selectivity before running tests.

        Returns:
            Formatted string with all parameters

        Example:
            >>> arr = NumericArrayRange(0, 999, array_size=3, frequency=100)
            >>> arr.set_max_count(100000)
            >>> print(arr.describe())
        """
        range_size = self.max_value - self.min_value
        max_possible_distinct = range_size + 1

        lines = [
            "=" * 50,
            "NumericArrayRange Configuration",
            "=" * 50,
            f"  Value range:        [{self.min_value}, {self.max_value}]",
            f"  Range size:         {range_size}",
            f"  Max possible distinct: {max_possible_distinct}",
            f"  Array size:         {self.array_size}",
            f"  Step size:          {self.step_size}",
            f"  Insertion order:    {self.insertion_order.value}",
            "",
        ]

        if self.max_count is not None:
            total_slots = self.max_count * self.array_size
            actual_frequency = total_slots // self._num_distinct if self._num_distinct else "N/A"
            docs_per_value = actual_frequency  # each appearance is in one (doc, pos) slot
            selectivity_pct = (docs_per_value / self.max_count * 100) if self.max_count > 0 else 0

            lines += [
                f"  Num docs:           {self.max_count:,}",
                f"  Total elem slots:   {total_slots:,}  (num_docs × array_size)",
                f"  Num distinct:       {self._num_distinct:,}",
                f"  Requested frequency:{self._explicit_frequency or 'auto'}",
                f"  Actual frequency:   {actual_frequency}  (total_slots / num_distinct)",
                f"  Docs per value:     ~{actual_frequency}  (≈ frequency, each value in ~N docs)",
                f"  Selectivity:        ~{selectivity_pct:.2f}%  (docs_per_value / num_docs)",
                "",
                "  How it's calculated:",
                f"    total_slots     = {self.max_count} × {self.array_size} = {total_slots}",
                f"    num_distinct    = min(total_slots/frequency, range+1)",
                f"                    = min({total_slots}/{self._explicit_frequency or 1}, {max_possible_distinct})",
                f"                    = {self._num_distinct}",
                f"    step_size       = range / (num_distinct - 1)",
                f"                    = {range_size} / {max(1, self._num_distinct - 1)}",
                f"                    = {self.step_size}",
                f"    actual_freq     = total_slots / num_distinct",
                f"                    = {total_slots} / {self._num_distinct}",
                f"                    = {actual_frequency}",
            ]

            if self._num_distinct < max_possible_distinct and self._explicit_frequency:
                needed_range = self._explicit_frequency and (total_slots // self._explicit_frequency - 1)
                lines.append(f"    ⚠ Range capped num_distinct. For exact frequency={self._explicit_frequency}, use max_value>={needed_range}")
        else:
            lines += [
                f"  Num docs:           NOT SET (call set_max_count())",
                f"  Num distinct:       {self._num_distinct}",
            ]

        lines.append("=" * 50)
        return "\n".join(lines)
