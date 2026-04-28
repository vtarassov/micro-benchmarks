"""
Tests for string value range implementations.
"""

import pytest
import string
from src.value_range import FixedLengthStringRange, InsertionOrder


class TestFixedLengthStringRange:
    """Tests for FixedLengthStringRange class."""

    def test_default_alphabet(self):
        """Test with default alphabet (ascii_letters)."""
        str_range = FixedLengthStringRange(length=3)

        # First string should be "aaa" (all first character)
        assert str_range.generate(0) == "aaa"

        # Second string should be "aab"
        assert str_range.generate(1) == "aab"

        # Verify length is correct
        assert len(str_range.generate(0)) == 3
        assert len(str_range.generate(100)) == 3

    def test_binary_alphabet(self):
        """Test with binary alphabet."""
        str_range = FixedLengthStringRange(length=3, alphabet="01")

        # Test binary counting: 000, 001, 010, 011, 100, 101, 110, 111
        assert str_range.generate(0) == "000"
        assert str_range.generate(1) == "001"
        assert str_range.generate(2) == "010"
        assert str_range.generate(3) == "011"
        assert str_range.generate(4) == "100"
        assert str_range.generate(5) == "101"
        assert str_range.generate(6) == "110"
        assert str_range.generate(7) == "111"

    def test_custom_alphabet(self):
        """Test with custom alphabet."""
        str_range = FixedLengthStringRange(length=2, alphabet="ABC")

        # Test base-3 counting with ABC
        assert str_range.generate(0) == "AA"
        assert str_range.generate(1) == "AB"
        assert str_range.generate(2) == "AC"
        assert str_range.generate(3) == "BA"
        assert str_range.generate(4) == "BB"
        assert str_range.generate(5) == "BC"
        assert str_range.generate(6) == "CA"
        assert str_range.generate(7) == "CB"
        assert str_range.generate(8) == "CC"

    def test_frequency_calculation(self):
        """Test that frequency (number of distinct values) is calculated correctly."""
        # Binary with length 3: 2^3 = 8 distinct values
        str_range = FixedLengthStringRange(length=3, alphabet="01")
        assert str_range.frequency == 8

        # ABC with length 2: 3^2 = 9 distinct values
        str_range = FixedLengthStringRange(length=2, alphabet="ABC")
        assert str_range.frequency == 9

        # Default alphabet (52 letters) with length 2: 52^2 = 2704 distinct values
        str_range = FixedLengthStringRange(length=2)
        assert str_range.frequency == 52 * 52

    def test_wrapping(self):
        """Test that ordinals wrap around after frequency."""
        str_range = FixedLengthStringRange(length=2, alphabet="AB")
        # 2^2 = 4 distinct values: AA, AB, BA, BB

        assert str_range.generate(0) == "AA"
        assert str_range.generate(1) == "AB"
        assert str_range.generate(2) == "BA"
        assert str_range.generate(3) == "BB"

        # Should wrap back to beginning
        assert str_range.generate(4) == "AA"
        assert str_range.generate(5) == "AB"

    def test_deterministic(self):
        """Test that generation is deterministic."""
        str_range = FixedLengthStringRange(length=4, alphabet="xyz")

        # Generate multiple times - should always be the same
        for ordinal in [0, 10, 100, 1000]:
            value1 = str_range.generate(ordinal)
            value2 = str_range.generate(ordinal)
            value3 = str_range.generate(ordinal)
            assert value1 == value2 == value3

    def test_hex_strings(self):
        """Test generating hex-like strings."""
        str_range = FixedLengthStringRange(length=2, alphabet="0123456789ABCDEF")

        assert str_range.generate(0) == "00"
        assert str_range.generate(1) == "01"
        assert str_range.generate(15) == "0F"
        assert str_range.generate(16) == "10"
        assert str_range.generate(255) == "FF"

    def test_invalid_length(self):
        """Test that invalid length raises error."""
        with pytest.raises(ValueError, match="length must be positive"):
            FixedLengthStringRange(length=0)

        with pytest.raises(ValueError, match="length must be positive"):
            FixedLengthStringRange(length=-1)

    def test_empty_alphabet(self):
        """Test that empty alphabet raises error."""
        with pytest.raises(ValueError, match="alphabet cannot be empty"):
            FixedLengthStringRange(length=3, alphabet="")

    def test_insertion_order_ascending(self):
        """Test ascending insertion order."""
        str_range = FixedLengthStringRange(length=2, alphabet="AB", insertion_order=InsertionOrder.ASCENDING)
        str_range.set_max_count(4)

        # Ascending: ordinals used directly
        assert str_range.allocate(0) == "AA"
        assert str_range.allocate(1) == "AB"
        assert str_range.allocate(2) == "BA"
        assert str_range.allocate(3) == "BB"

    def test_insertion_order_descending(self):
        """Test descending insertion order."""
        str_range = FixedLengthStringRange(length=2, alphabet="AB", insertion_order=InsertionOrder.DESCENDING)
        str_range.set_max_count(4)

        # Descending: ordinals reversed
        assert str_range.allocate(0) == "BB"  # max_count - 0 - 1 = 3
        assert str_range.allocate(1) == "BA"  # max_count - 1 - 1 = 2
        assert str_range.allocate(2) == "AB"  # max_count - 2 - 1 = 1
        assert str_range.allocate(3) == "AA"  # max_count - 3 - 1 = 0

    def test_get_method(self):
        """Test get method (no side effects)."""
        str_range = FixedLengthStringRange(length=2, alphabet="01")

        # get() should work without max_count set
        value1 = str_range.get(5)
        value2 = str_range.get(5)
        assert value1 == value2
        assert value1 == "01"  # Binary counting with length=2: ordinal 5 % 4 = 1, maps to "01"

        # allocate() works similarly when max_count is set
        str_range.set_max_count(10)
        result = str_range.allocate(0)
        assert result == "00"

    def test_callable_interface(self):
        """Test that ValueRange can be called as a function."""
        str_range = FixedLengthStringRange(length=2, alphabet="ABC")

        # Should be able to call as function
        assert str_range(0) == "AA"
        assert str_range(5) == "BC"

    def test_large_ordinals(self):
        """Test with large ordinal values."""
        str_range = FixedLengthStringRange(length=5, alphabet="0123456789")

        # Should handle large ordinals
        value = str_range.generate(99999)
        assert len(value) == 5
        assert all(c in "0123456789" for c in value)
        assert value == "99999"  # 10^5 range, so 99999 is still within

        # Test wrapping for ordinals beyond frequency
        value_wrapped = str_range.generate(100000)  # This wraps back to 0
        assert value_wrapped == "00000"
