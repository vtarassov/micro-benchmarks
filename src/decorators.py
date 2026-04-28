"""
Decorators for MongoDB performance testing framework.

This module provides decorators for defining document shapes with weights
for weighted random selection during load testing.
"""

from typing import Callable, Optional


def document_shape(weight: int = 1, max_count: Optional[int] = None):
    """
    Decorator for marking methods as document shapes with weights.

    Args:
        weight: The weight for this document shape (default: 1). Higher weights mean
                this shape will be selected more frequently during data generation.
        max_count: Optional maximum number of documents to generate with this shape.
                   If not provided, it will be calculated based on weight and the
                   global document_count, rounded up.

    Example:
        @document_shape(weight=70, max_count=7000)
        def simple_product(self, ctx):
            ...

        @document_shape(weight=30)  # max_count calculated automatically
        def complex_product(self, ctx):
            ...
    """
    def decorator(func: Callable) -> Callable:
        func._document_shape_weight = weight
        func._document_shape_max_count = max_count
        return func
    return decorator


def workload(weight: int = 1, name: Optional[str] = None):
    """
    Decorator for marking methods as workload operations with weights.

    Args:
        weight: The weight for this workload (default: 1). Higher weights mean
                this workload will be selected more frequently.
        name: Optional name for the workload. If not provided, defaults to the
              method name.

    Example:
        @workload(weight=70, name="read_product")
        def read_products(self):
            ...

        @workload(weight=30)  # name defaults to "write_product"
        def write_product(self):
            ...
    """
    def decorator(func: Callable) -> Callable:
        func._workload_weight = weight
        func._workload_name = name if name is not None else func.__name__
        return func
    return decorator


def pre_load(func: Callable) -> Callable:
    """
    Decorator for marking a method to be executed during the PRE_LOAD phase.

    The PRE_LOAD phase runs before data loading begins. Only the leader
    (user with ID 0) executes this method. Use this for setup tasks like:
    - Creating indexes
    - Dropping/recreating collections
    - Setting up database configuration

    Example:
        @pre_load
        def setup_indexes(self):
            self.collection.create_index([("category", 1)])
            self.collection.create_index([("price", 1)])
    """
    func._is_pre_load = True
    return func


def post_load(func: Callable) -> Callable:
    """
    Decorator for marking a method to be executed during the POST_LOAD phase.

    The POST_LOAD phase runs after all data loading is complete. Only the leader
    (user with ID 0) executes this method. Use this for tasks like:
    - Creating additional indexes after bulk insert
    - Running validation or verification
    - Warming up caches

    Example:
        @post_load
        def create_final_indexes(self):
            self.collection.create_index([("created_at", -1)])
            print(f"Total documents: {self.collection.count_documents({})}")
    """
    func._is_post_load = True
    return func
