"""
MongoDB Performance Test Framework

A Locust-based framework for performance testing MongoDB databases.
"""

from .perf_test_user import PerfTestUser
from .decorators import document_shape
from .value_range import ValueRange, IntegerRange

__all__ = [
    'PerfTestUser',
    'document_shape',
    'ValueRange',
    'IntegerRange'
]
__version__ = '0.1.0'
