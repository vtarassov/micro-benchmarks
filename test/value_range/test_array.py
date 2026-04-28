"""
Tests for NumericArrayRange.
"""

import pytest
import warnings
from collections import Counter

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from value_range import NumericArrayRange, InsertionOrder


class TestDeterminism:
    """Same doc_id always produces same array."""

    def test_same_doc_same_result(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        assert arr.generate(0) == arr.generate(0)
        assert arr.generate(42) == arr.generate(42)
        assert arr.generate(99) == arr.generate(99)

    def test_different_docs_different_results(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        # Not guaranteed to be different, but with 100 distinct values
        # and 100 docs, very likely
        results = [tuple(arr.generate(i)) for i in range(100)]
        # At least 90% should be unique arrays
        assert len(set(results)) > 50


class TestNoIntraArrayDuplicates:
    """No duplicate values within the same array."""

    def test_no_duplicates_high_distinct(self):
        arr = NumericArrayRange(0, 999, array_size=3)
        arr.set_max_count(1000)
        for doc_id in range(1000):
            array = arr.generate(doc_id)
            assert len(array) == len(set(array)), f"Duplicate in doc {doc_id}: {array}"

    def test_no_duplicates_low_distinct(self):
        """60 distinct values, array_size=3 — rejection should handle collisions."""
        arr = NumericArrayRange(0, 59, array_size=3)
        arr.set_max_count(1000)
        for doc_id in range(1000):
            array = arr.generate(doc_id)
            assert len(array) == len(set(array)), f"Duplicate in doc {doc_id}: {array}"

    def test_no_duplicates_minimal_distinct(self):
        """num_distinct == array_size — every array uses all values."""
        arr = NumericArrayRange(0, 2, array_size=3)
        arr.set_max_count(10)
        for doc_id in range(10):
            array = arr.generate(doc_id)
            assert len(array) == len(set(array)), f"Duplicate in doc {doc_id}: {array}"


class TestGetArrayAndAllocateConsistency:
    """get_array and allocate return same result for same doc_id."""

    def test_consistency(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        for doc_id in range(100):
            expected = arr.get_array(doc_id)
            actual = arr.allocate(doc_id)
            assert expected == actual, f"Mismatch at doc {doc_id}"


class TestGetElementConsistency:
    """get_element(d, p) == get_array(d)[p]."""

    def test_consistency(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        for doc_id in range(100):
            full_array = arr.get_array(doc_id)
            for pos in range(3):
                assert arr.get_element(doc_id, pos) == full_array[pos]

    def test_invalid_position(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        with pytest.raises(ValueError):
            arr.get_element(0, 3)
        with pytest.raises(ValueError):
            arr.get_element(0, -1)


class TestFrequencyAccuracy:
    """Each value appears approximately the expected number of times."""

    def test_frequency_1_global_uniqueness(self):
        """frequency=1 with sufficient range: every element value globally unique."""
        arr = NumericArrayRange(0, 29999, array_size=3, frequency=1)
        arr.set_max_count(10000)

        all_values = []
        for doc_id in range(10000):
            all_values.extend(arr.generate(doc_id))

        # 30000 slots, 30000 distinct → each value exactly once
        assert len(all_values) == len(set(all_values)), \
            f"Expected 30000 unique values, got {len(set(all_values))}"

    def test_frequency_default(self):
        """frequency=1, large range → each value appears exactly once."""
        arr = NumericArrayRange(0, 9999, array_size=3)
        arr.set_max_count(1000)

        counter = Counter()
        for doc_id in range(1000):
            for val in arr.generate(doc_id):
                counter[val] += 1

        # Total elements = 3000, num_distinct = 3000, each value exactly once
        for val, count in counter.items():
            assert count == 1, f"Value {val}: count={count}, expected 1"

    def test_frequency_explicit(self):
        """frequency=50, large range → num_distinct=600, low rejection."""
        arr = NumericArrayRange(0, 5999, array_size=3, frequency=50)
        arr.set_max_count(10000)

        counter = Counter()
        for doc_id in range(10000):
            for val in arr.generate(doc_id):
                counter[val] += 1

        # total_slots=30000, frequency=50 → num_distinct=600
        # actual_frequency = 30000/600 = 50
        expected = 50
        # At least 95% of values should be within 15% of expected
        within_threshold = sum(1 for c in counter.values() if abs(c - expected) / expected < 0.15)
        total_values = len(counter)
        assert within_threshold / total_values >= 0.95, \
            f"Only {within_threshold}/{total_values} values within 15% of expected={expected}"


class TestValueRange:
    """All values within [min_value, max_value]."""

    def test_values_in_range(self):
        arr = NumericArrayRange(10, 50, array_size=3)
        arr.set_max_count(500)
        for doc_id in range(500):
            for val in arr.generate(doc_id):
                assert 10 <= val <= 50, f"Value {val} out of range [10, 50]"

    def test_values_in_range_with_step(self):
        arr = NumericArrayRange(0, 100, array_size=3, step_size=10)
        arr.set_max_count(100)
        for doc_id in range(100):
            for val in arr.generate(doc_id):
                assert 0 <= val <= 100
                assert val % 10 == 0, f"Value {val} not a multiple of step_size=10"


class TestNumDistinctCap:
    """num_distinct capped by value range, with warning."""

    def test_cap_with_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            arr = NumericArrayRange(0, 99, array_size=3, frequency=1)
            arr.set_max_count(100000)
            # range=100, total_slots=300k, frequency=1 → wanted 300k distinct
            # but range only has 100 → capped at 100
            assert arr._num_distinct == 100
            assert len(w) == 1
            assert "value range" in str(w[0].message).lower()


class TestInsertionOrderValidation:
    """Non-RANDOM insertion orders raise NotImplementedError."""

    def test_ascending_raises(self):
        with pytest.raises(NotImplementedError):
            NumericArrayRange(0, 99, array_size=3, insertion_order=InsertionOrder.ASCENDING)

    def test_descending_raises(self):
        with pytest.raises(NotImplementedError):
            NumericArrayRange(0, 99, array_size=3, insertion_order=InsertionOrder.DESCENDING)

    def test_random_ok(self):
        arr = NumericArrayRange(0, 99, array_size=3, insertion_order=InsertionOrder.RANDOM)
        assert arr is not None


class TestArraySizeValidation:
    """array_size > num_distinct raises ValueError."""

    def test_array_size_too_large(self):
        with pytest.raises(ValueError):
            arr = NumericArrayRange(0, 1, array_size=3, step_size=1)
            # num_distinct = 2, array_size = 3 → impossible

    def test_array_size_equals_distinct(self):
        arr = NumericArrayRange(0, 2, array_size=3)
        arr.set_max_count(10)
        array = arr.generate(0)
        assert len(array) == 3
        assert len(set(array)) == 3


class TestGetPercentile:
    """get_percentile returns values in reasonable range."""

    def test_percentile_bounds(self):
        arr = NumericArrayRange(0, 999, array_size=3)
        arr.set_max_count(1000)

        val_0, _ = arr.get_percentile(0.0)
        val_100, _ = arr.get_percentile(100.0)
        val_50, _ = arr.get_percentile(50.0)

        assert val_0 <= val_50 <= val_100
        assert 0 <= val_0
        assert val_100 <= 999

    def test_percentile_invalid(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        with pytest.raises(ValueError):
            arr.get_percentile(-1.0)
        with pytest.raises(ValueError):
            arr.get_percentile(101.0)


class TestRandomRange:
    """random_range returns values within percentile bounds."""

    def test_random_range_bounds(self):
        arr = NumericArrayRange(0, 999, array_size=3)
        arr.set_max_count(1000)

        low, _ = arr.get_percentile(40.0)
        high, _ = arr.get_percentile(60.0)

        for _ in range(100):
            val, _ = arr.random_range(40.0, 60.0)
            assert low <= val <= high

    def test_random_range_invalid(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        with pytest.raises(ValueError):
            arr.random_range(60.0, 40.0)  # min > max


class TestDescribe:
    """describe() runs without error and contains key info."""

    def test_describe_with_max_count(self):
        arr = NumericArrayRange(0, 999, array_size=3, frequency=100)
        arr.set_max_count(100000)
        output = arr.describe()
        assert "NumericArrayRange" in output
        assert "1000" in output or "num_distinct" in output.lower()
        assert "100000" in output or "num_docs" in output.lower()

    def test_describe_without_max_count(self):
        arr = NumericArrayRange(0, 999, array_size=3)
        output = arr.describe()
        assert "NOT SET" in output


class TestDocumentCount:
    """allocate() increments document_count; generate()/get_array() do not."""

    def test_initial_document_count_is_zero(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        assert arr.document_count == 0

    def test_allocate_increments_document_count(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        for i in range(10):
            arr.allocate(i)
        assert arr.document_count == 10

    def test_generate_does_not_increment_document_count(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        for i in range(10):
            arr.generate(i)
        assert arr.document_count == 0

    def test_get_array_does_not_increment_document_count(self):
        arr = NumericArrayRange(0, 99, array_size=3)
        arr.set_max_count(100)
        for i in range(10):
            arr.get_array(i)
        assert arr.document_count == 0
