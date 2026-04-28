"""
Value Range module for deterministic data generation.

This module provides classes to map integers {0..N} deterministically into
various data types with specific generation parameters. This is useful for
performance testing where reproducible data generation is critical.

The key concept is that given the same ordinal input and parameters, the
ValueRange will always produce the same output value, enabling:
- Reproducible test data across runs
- Consistent shard key distributions
- Deterministic document generation based on ordinal values
"""

from typing import Any, Optional, Dict
from abc import ABC, abstractmethod
from enum import Enum
import random as python_random

from .utils import FastFeistelShuffler


class InsertionOrder(Enum):
    """Defines the order in which values are inserted during document generation."""
    ASCENDING = "ascending"
    DESCENDING = "descending"
    RANDOM = "random"


class ValueRange(ABC):
    """
    Abstract base class for value ranges.

    A ValueRange encapsulates the logic to deterministically map an ordinal
    (typically 0 to N) into a specific data type with configurable generation
    parameters.
    """

    def __init__(self, insertion_order: InsertionOrder = InsertionOrder.ASCENDING):
        """
        Initialize base ValueRange.

        Args:
            insertion_order: The order in which values are inserted (default: ASCENDING)
        """
        self.frequency: Optional[int] = None  # Number of distinct values, computed by subclasses
        self.max_count: Optional[int] = None
        self.insertion_order: InsertionOrder = insertion_order
        self._owning_shape: Optional[str] = None  # Track which shape owns this range
        self.shuffle: Optional[FastFeistelShuffler] = None  # Lazy-initialized for RANDOM insertion order

    def set_max_count(self, count: int) -> 'ValueRange':
        """
        Set the maximum number of documents to be generated.

        This can be used to specify how many documents will use this value range,
        which is required for DESCENDING insertion order and useful for validation.

        Args:
            count: Maximum number of documents

        Returns:
            Self for method chaining
        """
        self.max_count = count
        return self

    @abstractmethod
    def generate(self, ordinal: int) -> Any:
        """
        Generate a value deterministically from an integer ordinal.

        Args:
            ordinal: Integer ordinal

        Returns:
            Generated value of the appropriate type
        """
        pass

    def allocate(self, ordinal: int) -> Any:
        """
        Allocate a value for a document being generated.

        Insertion order transformations:
        - ASCENDING: ordinal used directly
        - DESCENDING: ordinal transformed as (max_count - ordinal - 1)
        - RANDOM: ordinal transformed using Feistel shuffle

        Args:
            ordinal: The ordinal/index to generate for (typically document_number)

        Returns:
            Generated value

        Raises:
            ValueError: If insertion_order is DESCENDING/RANDOM and max_count is not set
        """
        
        # Apply insertion order transformation
        if self.insertion_order == InsertionOrder.ASCENDING:
            return self.generate(ordinal)
        elif self.insertion_order == InsertionOrder.DESCENDING:
            if self.max_count is None:
                raise ValueError(
                    "Cannot use DESCENDING insertion order: max_count must be set"
                )
            transformed_ordinal = self.max_count - ordinal - 1
            return self.generate(transformed_ordinal)
        elif self.insertion_order == InsertionOrder.RANDOM:
            if self.max_count is None:
                raise ValueError(
                    "Cannot use RANDOM insertion order: max_count must be set"
                )
            if self.shuffle is None:
                self.shuffle = FastFeistelShuffler(max_count=self.max_count)

            transformed_ordinal = self.shuffle.get(ordinal)
            return self.generate(transformed_ordinal)
        else:
            # Fallback to ascending
            return self.generate(ordinal)

    def get(self, ordinal: int) -> Any:
        """
        Get a value by ordinal without tracking.

        Use this for querying or when you need to retrieve a value without side effects.

        Args:
            ordinal: The ordinal/index to generate for

        Returns:
            Generated value
        """
        return self.generate(ordinal)

    def random(self) -> Any:
        """
        Get a random value from this range.

        Chooses a random ordinal from 0 to max_count-1 and generates
        a value for that ordinal. This ensures the random value corresponds
        to a document that was actually generated.

        Returns:
            A random value from the range based on generated documents

        Raises:
            ValueError: If max count not set
        """
        if self.max_count is None:
            raise ValueError("Cannot get random value: no documents have been generated yet")
        random_ordinal = python_random.randint(0, self.max_count - 1)
        return self.generate(random_ordinal)

    def get_percentile(self, percentile: float) -> tuple[Any, int]:
        """
        Get the value and ordinal at a specific percentile of generated documents.

        The percentile is calculated based on the actual documents generated
        (max_count), using ascending ordinal order regardless of insertion_order.

        Args:
            percentile: The percentile to query (0.0 to 100.0)

        Returns:
            Tuple of (value, ordinal) at the specified percentile

        Raises:
            ValueError: If no documents generated or percentile out of range

        Example:
            >>> vr = int_range(0, 1000).set_max_count(1000)
            >>> # After generating documents...
            >>> value, ordinal = vr.get_percentile(50.0)  # Get median
            >>> value, ordinal = vr.get_percentile(90.0)  # Get p90
        """
        if self.max_count is None:
            raise ValueError(
                "Cannot get percentile: max_count not set"
            )

        if percentile < 0.0 or percentile > 100.0:
            raise ValueError(f"Percentile must be between 0.0 and 100.0, got {percentile}")

        # Calculate ordinal at this percentile
        # Use max_count - 1 since ordinals are 0-indexed
        ordinal = int((percentile / 100.0) * (self.max_count - 1))
        value = self.generate(ordinal)
        return (value, ordinal)

    def random_range(
        self,
        min_percentile: float = 0.0,
        max_percentile: float = 100.0
    ) -> tuple[Any, int]:
        """
        Get a random value within a percentile range.

        Selects a random ordinal within the specified percentile range and
        returns both the generated value and the ordinal. Percentiles are
        based on ascending ordinal order regardless of insertion_order.

        Args:
            min_percentile: Lower bound percentile (0.0 to 100.0, inclusive)
            max_percentile: Upper bound percentile (0.0 to 100.0, inclusive)

        Returns:
            Tuple of (value, ordinal) for a random ordinal within the percentile range

        Raises:
            ValueError: If percentiles are invalid or no documents generated

        Example:
            >>> # Get random value above p90
            >>> value, ordinal = vr.random_range(90.0, 100.0)

            >>> # Get random value between p40 and p60
            >>> value, ordinal = vr.random_range(40.0, 60.0)
        """
        if self.max_count is None:
            raise ValueError(
                "Cannot get random value from range: max_count not set"
            )

        if min_percentile < 0.0 or min_percentile > 100.0:
            raise ValueError(f"min_percentile must be between 0.0 and 100.0, got {min_percentile}")

        if max_percentile < 0.0 or max_percentile > 100.0:
            raise ValueError(f"max_percentile must be between 0.0 and 100.0, got {max_percentile}")

        if min_percentile > max_percentile:
            raise ValueError(
                f"min_percentile ({min_percentile}) cannot be greater than max_percentile ({max_percentile})"
            )

        # Calculate ordinal bounds for the percentile range
        min_ordinal = int((min_percentile / 100.0) * (self.max_count - 1))
        max_ordinal = int((max_percentile / 100.0) * (self.max_count - 1))

        # Select a random ordinal within the range
        random_ordinal = python_random.randint(min_ordinal, max_ordinal)
        value = self.generate(random_ordinal)

        return (value, random_ordinal)

    def __call__(self, index: int) -> Any:
        """Allow ValueRange to be called as a function."""
        return self.generate(index)


def process_document_shape(
    doc: Dict[str, Any],
    ordinal: int,
    max_count: Optional[int] = None,
    shape_id: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Process a document shape to replace ValueRange instances with actual values.

    This function walks through the document structure and:
    1. Tracks which shape "owns" each ValueRange (first shape to use it)
    2. Validates that ValueRanges are not shared across multiple shapes
    3. Automatically sets max_count on ValueRanges that don't have it set (from shape metadata)
    4. Calls allocate(ordinal) to replace the ValueRange with an actual value and track usage

    Assumptions:
        Setting ``max_count = shape_max_count`` assumes that every ValueRange
        in the document is used by all documents of that shape. In other words,
        each ValueRange will be called with ordinals ``[0, shape_max_count)``.

    Args:
        doc: The document to process
        ordinal: The ordinal/index to generate values for (typically shape_ordinal)
        max_count: The maximum number of documents for this shape (automatically set on ValueRanges)
        shape_id: Identifier for the shape (e.g., method name) to track ownership
        dry_run: If True, only configure ValueRange instances (set max_count and
                 _owning_shape) without calling allocate() or replacing values in
                 the doc. Used by the framework during on_start() to ensure
                 ValueRanges are ready even when data loading is skipped.
                 Default: False.

    Returns:
        The processed document with ValueRange instances replaced by actual values
        (when dry_run=False), or the original doc with ValueRanges still in place
        (when dry_run=True).

    Raises:
        ValueError: If a ValueRange is used by multiple document shapes

    Example:
        >>> price_range = IntegerRange(0, 100)
        >>> doc = {"price": price_range}
        >>> result = process_document_shape(doc, ordinal=5, max_count=1000, shape_id="simple_product")
        >>> # result["price"] will be an integer value (e.g., 5)
        >>> # price_range will have max_count=1000 set automatically
    """
    for key, value in doc.items():
        if isinstance(value, ValueRange):
            # Check ownership - ensure this ValueRange isn't used by multiple shapes
            if value._owning_shape is None:
                # First time using this range, claim ownership
                value._owning_shape = shape_id
            elif shape_id is not None and value._owning_shape != shape_id:
                # Different shape trying to use this range - error!
                raise ValueError(
                    f"ValueRange instance cannot be shared across multiple document shapes. "
                    f"This range is owned by '{value._owning_shape}' but is being used by '{shape_id}'. "
                    f"Create separate ValueRange instances for each document shape."
                )

            # Auto-set max_count if not already set and max_count provided
            if value.max_count is None and max_count is not None:
                value.set_max_count(max_count)
            # Replace the ValueRange with the generated value and track it
            # (skipped in dry_run mode - we only configured the range above)
            if not dry_run:
                doc[key] = value.allocate(ordinal=ordinal)
        elif isinstance(value, dict):
            # Recursively process nested dictionaries
            process_document_shape(value, ordinal, max_count, shape_id, dry_run)
        elif isinstance(value, list):
            # Process lists (checking each element)
            for idx, item in enumerate(value):
                if isinstance(item, ValueRange):
                    # Check ownership
                    if item._owning_shape is None:
                        item._owning_shape = shape_id
                    elif shape_id is not None and item._owning_shape != shape_id:
                        raise ValueError(
                            f"ValueRange instance cannot be shared across multiple document shapes. "
                            f"This range is owned by '{item._owning_shape}' but is being used by '{shape_id}'. "
                            f"Create separate ValueRange instances for each document shape."
                        )

                    # Auto-set max_count if not already set and max_count provided
                    if item.max_count is None and max_count is not None:
                        item.set_max_count(max_count)
                    # Replace the ValueRange with the generated value and track it
                    # (skipped in dry_run mode - we only configured the range above)
                    if not dry_run:
                        value[idx] = item.allocate(ordinal=ordinal)
                elif isinstance(item, dict):
                    process_document_shape(item, ordinal, max_count, shape_id, dry_run)

    return doc
