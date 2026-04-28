"""
String value range implementations.

This module provides string value range classes for deterministic data generation.
"""

import string as string_module
from typing import Optional
from .value_range import ValueRange, InsertionOrder


class FixedLengthStringRange(ValueRange):
    """
    Generates fixed-length strings from a specified alphabet.

    Maps integers {0..N} deterministically to strings of fixed length.
    Each ordinal is converted to a base-n representation where n is the alphabet size,
    similar to counting in that base.

    The maximum number of distinct strings is len(alphabet)^length.
    When ordinals exceed this, they wrap around using modulo.

    Examples:
        >>> str_range = FixedLengthStringRange(length=3)
        >>> # Uses default alphabet (a-zA-Z), generates: "aaa", "aab", "aac", ...
        >>> str_range.generate(0)   # Returns "aaa"
        >>> str_range.generate(1)   # Returns "aab"
        >>> str_range.generate(52)  # Returns "aba" (after wrapping a-z, A-Z)

        >>> str_range = FixedLengthStringRange(length=2, alphabet="01")
        >>> # Binary strings: "00", "01", "10", "11"
        >>> str_range.generate(0)   # Returns "00"
        >>> str_range.generate(1)   # Returns "01"
        >>> str_range.generate(2)   # Returns "10"
        >>> str_range.generate(3)   # Returns "11"
    """

    def __init__(
        self,
        length: int,
        alphabet: Optional[str] = None,
        insertion_order: InsertionOrder = InsertionOrder.ASCENDING
    ):
        """
        Initialize a fixed-length string range.

        Args:
            length: Length of generated strings (must be positive)
            alphabet: String of characters to use (default: string.ascii_letters)
            insertion_order: The order in which values are inserted (default: ASCENDING)

        Raises:
            ValueError: If length <= 0 or alphabet is empty
        """
        super().__init__(insertion_order=insertion_order)

        if length <= 0:
            raise ValueError(f"length must be positive, got {length}")

        if alphabet is None:
            alphabet = string_module.ascii_letters

        if len(alphabet) == 0:
            raise ValueError("alphabet cannot be empty")

        self.length = length
        self.alphabet = alphabet
        self.alphabet_size = len(alphabet)

        # Calculate the total number of distinct strings possible
        # This is alphabet_size^length
        self.frequency = self.alphabet_size ** length

    def generate(self, ordinal: int) -> str:
        """
        Generate a fixed-length string deterministically from ordinal.

        Converts the ordinal to a base-n representation where n is the alphabet size,
        then maps each digit to the corresponding alphabet character.

        Args:
            ordinal: Integer ordinal (will wrap if >= frequency)

        Returns:
            Generated string of fixed length
        """
        # Wrap ordinal if it exceeds the number of distinct values
        ordinal = ordinal % self.frequency

        # Convert ordinal to base-n representation
        # We build the string from right to left (least significant to most significant)
        result = []
        for _ in range(self.length):
            result.append(self.alphabet[ordinal % self.alphabet_size])
            ordinal //= self.alphabet_size

        # Reverse to get most significant digit first (left to right)
        result.reverse()
        return "".join(result)
