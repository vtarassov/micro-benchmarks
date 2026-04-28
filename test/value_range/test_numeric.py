"""
Pytest test suite for numeric value range classes.

Tests the IntegerRange class and related numeric value ranges.
Run with: pytest test/value_range/test_numeric.py -v
"""

import pytest
import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from value_range import IntegerRange, LongRange, FloatRange, InsertionOrder
from value_range.numeric import IntegerRange as DirectIntegerRange


class TestIntegerRange:
    """Test suite for IntegerRange class."""

    def test_basic_generation(self):
        """Test basic value generation."""
        ir = IntegerRange(0, 99)

        assert ir.generate(0) == 0
        assert ir.generate(50) == 50
        assert ir.generate(99) == 99

    def test_wraparound(self):
        """Test that values wrap around after max_value."""
        ir = IntegerRange(0, 99)

        # Should wrap around to 0
        assert ir.generate(100) == 0
        assert ir.generate(200) == 0

        # Should wrap to 50
        assert ir.generate(150) == 50

    def test_custom_range(self):
        """Test with non-zero minimum value."""
        ir = IntegerRange(100, 200)

        assert ir.generate(0) == 100
        assert ir.generate(50) == 150
        assert ir.generate(100) == 200
        assert ir.generate(101) == 100  # Wraps

    def test_single_value_range(self):
        """Test range with min_value == max_value."""
        ir = IntegerRange(42, 42)

        assert ir.generate(0) == 42
        assert ir.generate(100) == 42
        assert ir.generate(999) == 42

    def test_determinism(self):
        """Test that generation is deterministic."""
        ir = IntegerRange(0, 99)

        # Generate same values twice
        first_run = [ir.generate(i) for i in range(100)]
        second_run = [ir.generate(i) for i in range(100)]

        assert first_run == second_run

    def test_callable_interface(self):
        """Test that IntegerRange can be called as a function."""
        ir = IntegerRange(1, 10)

        assert ir(5) == ir.generate(5)
        assert ir(100) == ir.generate(100)

    def test_invalid_range(self):
        """Test that invalid range raises ValueError."""
        with pytest.raises(ValueError, match="min_value.*cannot be greater than max_value"):
            IntegerRange(100, 50)

    def test_negative_range(self):
        """Test range with negative values."""
        ir = IntegerRange(-50, 50)

        assert ir.generate(0) == -50
        assert ir.generate(50) == 0
        assert ir.generate(100) == 50
        assert ir.generate(101) == -50  # Wraps

    def test_large_range(self):
        """Test with a large range."""
        ir = IntegerRange(0, 1000000)

        assert ir.generate(0) == 0
        assert ir.generate(500000) == 500000
        assert ir.generate(1000000) == 1000000
        assert ir.generate(1000001) == 0  # Wraps

    def test_distribution(self):
        """Test that values are evenly distributed."""
        ir = IntegerRange(0, 9)

        # Generate 100 values (10 complete cycles)
        values = [ir.generate(i) for i in range(100)]

        # Each value 0-9 should appear exactly 10 times
        for digit in range(10):
            assert values.count(digit) == 10

    def test_modulo_behavior(self):
        """Test that the modulo behavior works correctly."""
        ir = IntegerRange(0, 9)

        # Test various indices that should map to the same values
        assert ir.generate(0) == ir.generate(10) == ir.generate(20)
        assert ir.generate(5) == ir.generate(15) == ir.generate(25)
        assert ir.generate(9) == ir.generate(19) == ir.generate(29)

    def test_frequency(self):
        """Test that frequency represents repetitions per value (defaults to 1 for all unique)."""
        # Default frequency is 1 (all values are unique, no repetitions)
        assert IntegerRange(0, 9).frequency == 1
        assert IntegerRange(0, 99).frequency == 1
        assert IntegerRange(100, 200).frequency == 1
        assert IntegerRange(42, 42).frequency == 1

        # Explicit frequency means each value repeats that many times
        ir = IntegerRange(0, 10, frequency=5)
        assert ir.frequency == 5
        # With frequency=5, indices 0-4 should all return the first value
        assert ir.generate(0) == ir.generate(1) == ir.generate(2) == ir.generate(3) == ir.generate(4)
        # Index 5 should start the next value
        assert ir.generate(5) != ir.generate(0)

    def test_import_from_numeric_module(self):
        """Test that IntegerRange can be imported directly from numeric module."""
        ir1 = IntegerRange(0, 10)
        ir2 = DirectIntegerRange(0, 10)

        assert type(ir1) == type(ir2)
        assert ir1.generate(5) == ir2.generate(5)


class TestInsertionOrder:
    """Test insertion order functionality."""

    def test_ascending_order(self):
        """Test ASCENDING insertion order."""
        ir = IntegerRange(0, 10, insertion_order=InsertionOrder.ASCENDING)
        ir.set_max_count(100)

        # Ascending should use ordinal directly
        assert ir.allocate(0) == 0
        assert ir.allocate(5) == 5
        assert ir.allocate(10) == 10

    def test_descending_order(self):
        """Test DESCENDING insertion order."""
        ir = IntegerRange(0, 10, insertion_order=InsertionOrder.DESCENDING)
        ir.set_max_count(100)

        # Descending reverses the ordinal
        # ordinal 0 -> (100 - 0 - 1) = 99 -> 99 % 11 = 0
        # ordinal 1 -> (100 - 1 - 1) = 98 -> 98 % 11 = 10
        assert ir.allocate(0) == 0
        assert ir.allocate(1) == 10

    def test_random_order(self):
        """Test RANDOM insertion order."""
        ir = IntegerRange(0, 100, insertion_order=InsertionOrder.RANDOM)
        ir.set_max_count(100)

        # Random should produce different but deterministic values
        val1 = ir.allocate(0)
        val2 = ir.allocate(1)

        # Values should be in range
        assert 0 <= val1 <= 100
        assert 0 <= val2 <= 100

        # Should be deterministic - reset and try again
        ir2 = IntegerRange(0, 100, insertion_order=InsertionOrder.RANDOM)
        ir2.set_max_count(100)

        assert ir2.allocate(0) == val1
        assert ir2.allocate(1) == val2

    def test_descending_requires_max_count(self):
        """Test that DESCENDING requires max_count to be set."""
        ir = IntegerRange(0, 10, insertion_order=InsertionOrder.DESCENDING)

        with pytest.raises(ValueError, match="max_count must be set"):
            ir.allocate(0)

    def test_random_requires_max_count(self):
        """Test that RANDOM requires max_count to be set."""
        ir = IntegerRange(0, 10, insertion_order=InsertionOrder.RANDOM)

        with pytest.raises(ValueError, match="max_count must be set"):
            ir.allocate(0)


class TestPercentileMethods:
    """Test suite for percentile-based query methods."""

    def test_get_percentile_basic(self):
        """Test basic get_percentile functionality."""
        ir = IntegerRange(0, 99)
        ir.set_max_count(100)

        # Generate 100 documents (0-99)
        for i in range(100):
            ir.allocate(i)

        # Test exact percentiles
        value, ordinal = ir.get_percentile(0.0)
        assert ordinal == 0
        assert value == 0

        value, ordinal = ir.get_percentile(50.0)
        assert ordinal == 49  # 50th percentile of 0-99 (100 items)
        assert value == 49

        value, ordinal = ir.get_percentile(100.0)
        assert ordinal == 99
        assert value == 99

    def test_get_percentile_median(self):
        """Test getting the median value."""
        ir = IntegerRange(0, 999)
        ir.set_max_count(1000)

        for i in range(1000):
            ir.allocate(i)

        value, ordinal = ir.get_percentile(50.0)
        assert ordinal == 499  # Median position
        assert value == 499

    def test_get_percentile_p90_p95_p99(self):
        """Test common percentiles (p90, p95, p99)."""
        ir = IntegerRange(0, 999)
        ir.set_max_count(1000)

        for i in range(1000):
            ir.allocate(i)

        # P90
        value, ordinal = ir.get_percentile(90.0)
        assert ordinal == 899
        assert value == 899

        # P95
        value, ordinal = ir.get_percentile(95.0)
        assert ordinal == 949
        assert value == 949

        # P99
        value, ordinal = ir.get_percentile(99.0)
        assert ordinal == 989
        assert value == 989

    def test_get_percentile_no_documents(self):
        """Test that get_percentile raises error when max_count not set."""
        ir = IntegerRange(0, 99)

        with pytest.raises(ValueError, match="max_count not set"):
            ir.get_percentile(50.0)

    def test_get_percentile_invalid_percentile(self):
        """Test that invalid percentile values raise errors."""
        ir = IntegerRange(0, 99)
        ir.set_max_count(100)

        with pytest.raises(ValueError, match="Percentile must be between 0.0 and 100.0"):
            ir.get_percentile(-1.0)

        with pytest.raises(ValueError, match="Percentile must be between 0.0 and 100.0"):
            ir.get_percentile(101.0)

    def test_get_percentile_with_wrapping(self):
        """Test get_percentile when values wrap around the range."""
        ir = IntegerRange(0, 9)  # Small range that will wrap
        ir.set_max_count(100)

        for i in range(100):
            ir.allocate(i)

        # P50 should be ordinal 49
        value, ordinal = ir.get_percentile(50.0)
        assert ordinal == 49
        assert value == 9  # 49 % 10 = 9

    def test_random_range_basic(self):
        """Test basic random_range functionality."""
        ir = IntegerRange(0, 99)
        ir.set_max_count(100)

        for i in range(100):
            ir.allocate(i)

        # Get random value in full range
        value, ordinal = ir.random_range(0.0, 100.0)
        assert 0 <= ordinal <= 99
        assert 0 <= value <= 99

    def test_random_range_above_p90(self):
        """Test getting random values above p90."""
        ir = IntegerRange(0, 999)
        ir.set_max_count(1000)

        for i in range(1000):
            ir.allocate(i)

        # Test multiple times to ensure all are in range
        for _ in range(10):
            value, ordinal = ir.random_range(90.0, 100.0)
            assert 899 <= ordinal <= 999
            assert 899 <= value <= 999

    def test_random_range_between_p40_p60(self):
        """Test getting random values between p40 and p60."""
        ir = IntegerRange(0, 999)
        ir.set_max_count(1000)

        for i in range(1000):
            ir.allocate(i)

        # Test multiple times
        for _ in range(10):
            value, ordinal = ir.random_range(40.0, 60.0)
            assert 399 <= ordinal <= 599
            assert 399 <= value <= 599

    def test_random_range_single_percentile(self):
        """Test random_range with same min and max percentile."""
        ir = IntegerRange(0, 999)
        ir.set_max_count(1000)

        for i in range(1000):
            ir.allocate(i)

        # Should always return the same ordinal
        value, ordinal = ir.random_range(50.0, 50.0)
        assert ordinal == 499
        assert value == 499

    def test_random_range_no_documents(self):
        """Test that random_range raises error when max_count not set."""
        ir = IntegerRange(0, 99)

        with pytest.raises(ValueError, match="max_count not set"):
            ir.random_range(0.0, 100.0)

    def test_random_range_invalid_percentiles(self):
        """Test that invalid percentile ranges raise errors."""
        ir = IntegerRange(0, 99)
        ir.set_max_count(100)

        # min_percentile < 0
        with pytest.raises(ValueError, match="min_percentile must be between 0.0 and 100.0"):
            ir.random_range(-1.0, 50.0)

        # max_percentile > 100
        with pytest.raises(ValueError, match="max_percentile must be between 0.0 and 100.0"):
            ir.random_range(50.0, 101.0)

        # min > max
        with pytest.raises(ValueError, match="min_percentile.*cannot be greater than max_percentile"):
            ir.random_range(60.0, 40.0)

    def test_random_range_determinism(self):
        """Test that random_range is properly random (not always the same)."""
        ir = IntegerRange(0, 999)
        ir.set_max_count(1000)

        for i in range(1000):
            ir.allocate(i)

        # Get 20 random values in the p40-p60 range
        ordinals = set()
        for _ in range(20):
            value, ordinal = ir.random_range(40.0, 60.0)
            ordinals.add(ordinal)

        # Should have gotten multiple different ordinals (not all the same)
        assert len(ordinals) > 1

    def test_percentile_with_small_document_count(self):
        """Test percentile methods with very small document counts."""
        # Use explicit step_size to control value generation
        ir = IntegerRange(0, 99, step_size=1)
        ir.set_max_count(5)

        # Generate only 5 documents
        for i in range(5):
            ir.allocate(i)

        # P0 should be ordinal 0
        value, ordinal = ir.get_percentile(0.0)
        assert ordinal == 0
        assert value == 0

        # P100 should be ordinal 4
        value, ordinal = ir.get_percentile(100.0)
        assert ordinal == 4
        assert value == 4

        # P50 should be ordinal 2
        value, ordinal = ir.get_percentile(50.0)
        assert ordinal == 2
        assert value == 2

    def test_percentile_practical_query_use_case(self):
        """Test practical use case: querying high-value items."""
        # Simulate a price range
        price_range = IntegerRange(100, 10000)  # $1.00 to $100.00 in cents
        price_range.set_max_count(10000)

        # Generate 10000 products
        for i in range(10000):
            price_range.allocate(i)

        # Find the price at p90 (expensive items)
        price, ordinal = price_range.get_percentile(90.0)
        assert price >= 9000  # Should be near the top of the range

        # Query for a random expensive item (top 10%)
        price, ordinal = price_range.random_range(90.0, 100.0)
        assert 9000 <= price <= 10000

    def test_percentile_with_insertion_order_descending(self):
        """Test that percentiles use ascending ordinals regardless of insertion_order."""
        ir = IntegerRange(0, 99, insertion_order=InsertionOrder.DESCENDING)
        ir.set_max_count(100)

        # Generate 100 documents with descending insertion order
        for i in range(100):
            ir.allocate(i)

        # Percentiles should still be based on ascending ordinals
        value, ordinal = ir.get_percentile(0.0)
        assert ordinal == 0
        assert value == 0  # generate(0) still returns 0

        value, ordinal = ir.get_percentile(100.0)
        assert ordinal == 99
        assert value == 99


class TestPracticalUseCases:
    """Test practical use cases for IntegerRange."""

    def test_product_quantities(self):
        """Test using IntegerRange for product quantities."""
        quantity_range = IntegerRange(0, 1000)

        # Simulate quantities for document numbers
        quantities = {
            0: quantity_range.generate(0),
            250: quantity_range.generate(250),
            500: quantity_range.generate(500),
            1000: quantity_range.generate(1000),
            1001: quantity_range.generate(1001),
        }

        assert quantities[0] == 0
        assert quantities[250] == 250
        assert quantities[500] == 500
        assert quantities[1000] == 1000  # Max value
        assert quantities[1001] == 0  # Wraps

    def test_price_in_cents(self):
        """Test using IntegerRange for prices in cents."""
        price_cents_range = IntegerRange(999, 99999)  # $9.99 to $999.99

        price_0 = price_cents_range.generate(0)
        assert price_0 == 999  # $9.99

        # Convert to dollars
        price_dollars = price_0 / 100.0
        assert price_dollars == 9.99

    def test_category_indices(self):
        """Test using IntegerRange for category selection."""
        categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Toys"]
        category_index_range = IntegerRange(0, len(categories) - 1)

        # Verify we can map document numbers to categories
        for doc_num in range(20):
            idx = category_index_range.generate(doc_num)
            assert 0 <= idx < len(categories)
            category = categories[idx]
            assert category in categories

    def test_rating_generation(self):
        """Test generating ratings (1.0 to 5.0) using integers."""
        # Store ratings as integers 10-50, divide by 10 to get decimal
        rating_range = IntegerRange(10, 50)

        rating_int = rating_range.generate(0)
        rating_decimal = rating_int / 10.0

        assert 1.0 <= rating_decimal <= 5.0
        assert rating_decimal == 1.0  # First value

    def test_consistent_across_workers(self):
        """Test that different workers generate the same values."""
        # Simulate two workers generating the same document
        worker1_range = IntegerRange(0, 1000)
        worker2_range = IntegerRange(0, 1000)

        doc_num = 12345

        worker1_value = worker1_range.generate(doc_num)
        worker2_value = worker2_range.generate(doc_num)

        assert worker1_value == worker2_value

    def test_shard_key_distribution(self):
        """Test using IntegerRange for shard key generation."""
        # Simulate 16 shards
        shard_range = IntegerRange(0, 15)

        # Generate shard keys for 160 documents
        shard_counts = {}
        for doc_num in range(160):
            shard = shard_range.generate(doc_num)
            shard_counts[shard] = shard_counts.get(shard, 0) + 1

        # Each shard should have exactly 10 documents
        for shard in range(16):
            assert shard_counts[shard] == 10


@pytest.mark.parametrize("min_val,max_val,index,expected", [
    (0, 10, 0, 0),
    (0, 10, 5, 5),
    (0, 10, 10, 10),
    (0, 10, 11, 0),
    (0, 10, 21, 10),
    (100, 200, 0, 100),
    (100, 200, 50, 150),
    (100, 200, 101, 100),
    (-10, 10, 0, -10),
    (-10, 10, 10, 0),
    (-10, 10, 20, 10),
])
def test_parametrized_generation(min_val, max_val, index, expected):
    """Parametrized test for various range configurations."""
    ir = IntegerRange(min_val, max_val)
    assert ir.generate(index) == expected


class TestLongRange:
    """Test suite for LongRange class."""

    def test_basic_generation(self):
        """Test basic long value generation."""
        lr = LongRange(0, 1000000)

        assert lr.generate(0) == 0
        assert lr.generate(500) == 500
        assert lr.generate(1000) == 1000

    def test_large_range(self):
        """Test with very large 64-bit integer range."""
        lr = LongRange(0, 2**63 - 1)

        assert lr.generate(0) == 0
        # With very large range, default step_size is computed to avoid huge ranges
        # It won't be consecutive, but should be deterministic and span the range
        assert lr.generate(1) > 0
        assert lr.generate(100) > lr.generate(1)

    def test_with_step_size(self):
        """Test LongRange with explicit step_size."""
        lr = LongRange(0, 1000000, step_size=10000)

        assert lr.generate(0) == 0
        assert lr.generate(1) == 10000
        assert lr.generate(10) == 100000
        assert lr.generate(100) == 1000000

    def test_with_frequency(self):
        """Test LongRange with frequency (repetitions)."""
        lr = LongRange(0, 100, frequency=5, step_size=10)

        # First 5 indices should return 0
        for i in range(5):
            assert lr.generate(i) == 0

        # Next 5 indices should return 10
        for i in range(5, 10):
            assert lr.generate(i) == 10

        # Next 5 indices should return 20
        for i in range(10, 15):
            assert lr.generate(i) == 20


    def test_determinism(self):
        """Test that long generation is deterministic."""
        lr = LongRange(0, 1000000)

        first_run = [lr.generate(i) for i in range(100)]
        second_run = [lr.generate(i) for i in range(100)]

        assert first_run == second_run

    def test_wraparound(self):
        """Test that values wrap around after exceeding range."""
        lr = LongRange(0, 99, step_size=10)

        # Should cycle through 0,10,20,30,40,50,60,70,80,90 (10 values)
        assert lr.generate(0) == 0
        assert lr.generate(10) == 0  # Wraps around
        assert lr.generate(11) == 10


class TestFloatRange:
    """Test suite for FloatRange class."""

    def test_basic_generation(self):
        """Test basic float value generation."""
        fr = FloatRange(0.0, 100.0, step_size=10.0)

        assert fr.generate(0) == 0.0
        assert fr.generate(1) == 10.0
        assert fr.generate(5) == 50.0
        assert fr.generate(10) == 100.0

    def test_with_frequency(self):
        """Test FloatRange with frequency (repetitions)."""
        fr = FloatRange(0.0, 10.0, frequency=3, step_size=2.0)

        # First 3 indices should return 0.0
        for i in range(3):
            assert fr.generate(i) == 0.0

        # Next 3 indices should return 2.0
        for i in range(3, 6):
            assert fr.generate(i) == 2.0

        # Next 3 indices should return 4.0
        for i in range(6, 9):
            assert fr.generate(i) == 4.0

    def test_small_step_size(self):
        """Test FloatRange with small step increments."""
        fr = FloatRange(0.0, 1.0, step_size=0.1)

        assert abs(fr.generate(0) - 0.0) < 0.001
        assert abs(fr.generate(1) - 0.1) < 0.001
        assert abs(fr.generate(5) - 0.5) < 0.001
        assert abs(fr.generate(10) - 1.0) < 0.001

    def test_negative_range(self):
        """Test FloatRange with negative values."""
        fr = FloatRange(-10.0, 10.0, step_size=5.0)

        assert fr.generate(0) == -10.0
        assert fr.generate(1) == -5.0
        assert fr.generate(2) == 0.0
        assert fr.generate(3) == 5.0
        assert fr.generate(4) == 10.0


    def test_determinism(self):
        """Test that float generation is deterministic."""
        fr = FloatRange(0.0, 100.0, step_size=1.5)

        first_run = [fr.generate(i) for i in range(50)]
        second_run = [fr.generate(i) for i in range(50)]

        assert first_run == second_run

    def test_wraparound(self):
        """Test that values wrap around correctly."""
        fr = FloatRange(0.0, 5.0, step_size=1.0)

        # Should cycle through 0.0, 1.0, 2.0, 3.0, 4.0, 5.0 (6 values)
        assert fr.generate(0) == 0.0
        assert fr.generate(6) == 0.0  # Wraps around
        assert fr.generate(7) == 1.0


class TestFrequencySemantics:
    """Test suite for frequency parameter semantics (repetitions per value)."""

    def test_frequency_default_is_one(self):
        """Test that default frequency is 1 (all unique values)."""
        ir = IntegerRange(0, 10)
        fr = FloatRange(0.0, 10.0, step_size=1.0)
        lr = LongRange(0, 10)

        assert ir.frequency == 1
        assert fr.frequency == 1
        assert lr.frequency == 1

    def test_frequency_means_repetitions(self):
        """Test that frequency represents how many times each value repeats."""
        ir = IntegerRange(0, 10, frequency=4, step_size=2)

        # With frequency=4, each value repeats 4 times
        # Sequence should be: 0,0,0,0, 2,2,2,2, 4,4,4,4, ...
        assert ir.generate(0) == 0
        assert ir.generate(1) == 0
        assert ir.generate(2) == 0
        assert ir.generate(3) == 0
        assert ir.generate(4) == 2
        assert ir.generate(5) == 2
        assert ir.generate(6) == 2
        assert ir.generate(7) == 2
        assert ir.generate(8) == 4

    def test_frequency_one_all_unique(self):
        """Test that frequency=1 produces all unique values."""
        ir = IntegerRange(0, 10, frequency=1, step_size=1)

        # All values should be unique
        values = [ir.generate(i) for i in range(11)]
        assert len(values) == len(set(values))  # All unique
        assert values == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    def test_high_frequency_high_repetition(self):
        """Test that high frequency means high repetition."""
        ir = IntegerRange(0, 5, frequency=100, step_size=1)

        # Each value repeats 100 times
        # First 100 values should all be 0
        for i in range(100):
            assert ir.generate(i) == 0

        # Next 100 values should all be 1
        for i in range(100, 200):
            assert ir.generate(i) == 1

    def test_frequency_with_max_count(self):
        """Test frequency behavior when max_count is set."""
        ir = IntegerRange(0, 100, frequency=10)
        ir.set_max_count(1000)

        # With frequency=10 and max_count=1000, we get 100 distinct values
        # Each value repeats 10 times
        assert ir.generate(0) == ir.generate(9)  # First value repeats
        assert ir.generate(10) != ir.generate(0)  # Next value is different


class TestStepSizeParameter:
    """Test suite for step_size parameter."""

    def test_step_size_controls_spacing(self):
        """Test that step_size controls spacing between distinct values."""
        ir = IntegerRange(0, 100, step_size=5)

        assert ir.generate(0) == 0
        assert ir.generate(1) == 5
        assert ir.generate(2) == 10
        assert ir.generate(20) == 100

    def test_step_size_with_frequency_one(self):
        """Test that step_size with frequency=1 (default) produces spaced unique values."""
        fr = FloatRange(0.0, 10.0, step_size=0.5)

        # Should generate: 0.0, 0.5, 1.0, 1.5, 2.0, ...
        assert fr.frequency == 1
        values = [fr.generate(i) for i in range(10)]

        # Check spacing
        for i in range(1, 10):
            assert abs(values[i] - values[i-1] - 0.5) < 0.001

    def test_large_step_size(self):
        """Test with large step_size that skips many values."""
        lr = LongRange(0, 1000000, step_size=100000)

        assert lr.generate(0) == 0
        assert lr.generate(1) == 100000
        assert lr.generate(2) == 200000
        assert lr.generate(10) == 1000000

    def test_step_size_only_defaults_frequency_to_one(self):
        """Test that providing only step_size defaults frequency to 1."""
        ir = IntegerRange(0, 100, step_size=10)

        assert ir.frequency == 1
        assert ir.step_size == 10


class TestCombinedFrequencyAndStepSize:
    """Test suite for combined frequency and step_size parameters."""

    def test_frequency_and_step_size_together(self):
        """Test using both frequency and step_size together."""
        ir = IntegerRange(0, 100, frequency=5, step_size=10)

        # Each value (0, 10, 20, ...) should repeat 5 times
        # Sequence: 0,0,0,0,0, 10,10,10,10,10, 20,20,20,20,20, ...

        for i in range(5):
            assert ir.generate(i) == 0

        for i in range(5, 10):
            assert ir.generate(i) == 10

        for i in range(10, 15):
            assert ir.generate(i) == 20

    def test_practical_use_case_category_distribution(self):
        """Test practical use case: distributing documents across categories."""
        # 10 categories, each document stays in same category for 100 inserts
        category_range = IntegerRange(0, 9, frequency=100, step_size=1)

        # First 100 documents in category 0
        categories = [category_range.generate(i) for i in range(100)]
        assert all(c == 0 for c in categories)

        # Next 100 documents in category 1
        categories = [category_range.generate(i) for i in range(100, 200)]
        assert all(c == 1 for c in categories)

    def test_practical_use_case_price_buckets(self):
        """Test practical use case: price distribution in buckets."""
        # Prices from $10 to $100, in $10 increments, each price used for 50 products
        price_range = FloatRange(10.0, 100.0, frequency=50, step_size=10.0)

        # First 50 products at $10
        prices = [price_range.generate(i) for i in range(50)]
        assert all(abs(p - 10.0) < 0.001 for p in prices)

        # Next 50 products at $20
        prices = [price_range.generate(i) for i in range(50, 100)]
        assert all(abs(p - 20.0) < 0.001 for p in prices)

    def test_practical_use_case_timestamp_batches(self):
        """Test practical use case: timestamp generation in batches."""
        # Timestamps in 1-hour increments, 3600 documents per timestamp
        timestamp_range = LongRange(0, 86400, frequency=3600, step_size=3600)

        # First 3600 documents at timestamp 0
        timestamps = [timestamp_range.generate(i) for i in range(3600)]
        assert all(t == 0 for t in timestamps)

        # Next 3600 documents at timestamp 3600 (1 hour later)
        timestamps = [timestamp_range.generate(i) for i in range(3600, 7200)]
        assert all(t == 3600 for t in timestamps)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
