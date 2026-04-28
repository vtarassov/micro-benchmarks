"""
Value Range package for deterministic data generation.

This package provides tools for creating deterministic, reproducible data
generation patterns for performance testing.
"""

from .value_range import (
    ValueRange,
    InsertionOrder,
    process_document_shape,
)

from .numeric import (
    NumericRange,
    IntegerRange,
    LongRange,
    FloatRange,
)

from .string import (
    FixedLengthStringRange,
)

from .array import (
    NumericArrayRange,
)

from .utils import (
    feistel_shuffle,
)

__all__ = [
    # Core classes
    'ValueRange',
    'NumericRange',
    'IntegerRange',
    'LongRange',
    'FloatRange',
    'NumericArrayRange',
    'FixedLengthStringRange',
    'InsertionOrder',

    # Helper functions
    'process_document_shape',

    # Utilities
    'feistel_shuffle',
]
