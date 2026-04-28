# DocumentDB Micro Benchmarks

A sophisticated performance testing framework specifically designed for document databases. This framework provides a flexible, extensible system for generating realistic test data, simulating complex workloads, and measuring database performance across different scenarios.

## Features

- **Multi-process coordination** - Shared memory coordination for parallel test execution
- **Weighted document generation** - Create diverse document shapes with configurable distributions
- **Configurable workloads** - Define and weight different database operations
- **Phase-based execution** - Structured test phases (PRE_LOAD, DATA_LOAD, POST_LOAD, WORKLOAD)
- **Thread-safe operations** - Atomic counters and semaphore-based synchronization
- **Decorator-based API** - Simple, declarative test definition

## Installation

### Prerequisites

- Python 3.9 or higher
- DocumentDB or MongoDB-compatible instance (local or remote)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/aws/documentdb-micro-benchmarks.git
cd documentdb-micro-benchmarks
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

Here's a simple example to get you started:

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from perf_test_user import PerfTestUser, document_shape, workload, pre_load, post_load
from value_range import IntegerRange, InsertionOrder


class MyPerformanceTest(PerfTestUser):
    abstract = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.price = IntegerRange(0, 10000, frequency=100, insertion_order=InsertionOrder.RANDOM)

    @pre_load
    def setup_indexes(self):
        """Create indexes before loading data"""
        self.collection.create_index([("category", 1)])
        self.collection.create_index([("price", 1)])

    @document_shape(weight=70, max_count=7000)
    def generate_simple_product(self, ctx):
        """Generate a simple product document (70% of documents)"""
        return {
            "name": f"Product {ctx.document_number}",
            "category": "electronics",
            "price": self.price
        }

    @document_shape(weight=30, max_count=3000)
    def generate_detailed_product(self, ctx):
        """Generate a detailed product document (30% of documents)"""
        return {
            "name": f"Premium Product {ctx.document_number}",
            "category": "premium",
            "price": self.price,
            "specs": {
                "brand": "TechCorp",
                "warranty": "2 years"
            }
        }

    @workload(weight=60, name="read_product")
    def read_products(self):
        """Read operation (60% of workload)"""
        self.collection.find_one({"category": "electronics"})

    @workload(weight=30, name="update_price")
    def update_product_price(self):
        """Update operation (30% of workload)"""
        self.collection.update_one(
            {"category": "electronics"},
            {"$set": {"price": self.price.random()}}
        )

    @workload(weight=10, name="delete_product")
    def delete_product(self):
        """Delete operation (10% of workload)"""
        self.collection.delete_one({"price": {"$lt": 30}})

    @post_load
    def validate_data(self):
        """Verify data after loading"""
        total_docs = self.collection.count_documents({})
        print(f"Total documents loaded: {total_docs}")
```

## Core Concepts

### Test Phases

The framework executes tests in four distinct phases:

1. **PRE_LOAD** - Initial setup (create indexes, prepare collections)
2. **DATA_LOAD** - Generate and insert test data using `@document_shape` methods
3. **POST_LOAD** - Final setup after data loading (validation, additional indexes)
4. **WORKLOAD** - Execute performance tests using `@workload` methods

### Decorators

#### `@document_shape(weight, max_count=None)`
Defines a method that generates a document structure. The `weight` parameter controls the distribution of document types.

```python
@document_shape(weight=50, max_count=5000)
def generate_user(self, ctx):
    return {"username": f"user_{ctx.document_number}"}
```

#### `@workload(weight, name=None)`
Defines a workload operation. The `weight` parameter determines how often this operation is executed relative to others.

```python
@workload(weight=40, name="read_user")
def read_user(self):
    self.collection.find_one({"username": {"$exists": True}})
```

#### `@pre_load`
Marks a method to be executed before data loading begins.

```python
@pre_load
def setup(self):
    self.collection.create_index([("username", 1)])
```

#### `@post_load`
Marks a method to be executed after data loading completes.

```python
@post_load
def verify(self):
    count = self.collection.count_documents({})
    assert count > 0, "No documents were loaded"
```

### Value Ranges

The framework supports value ranges for generating varied data:

```python
from value_range import IntegerRange, FloatRange, FixedLengthStringRange

@document_shape(weight=100)
def generate_product(self, ctx):
    return {
        "price": FloatRange(9.99, 999.99),
        "quantity": IntegerRange(1, 100),
        "sku": FixedLengthStringRange(length=8),
    }
```

`ValueRange` instances are placed directly in the document dict. The framework replaces them with actual values during document generation via `process_document_shape()`.

## Configuration

### Weight Distribution

Weights control how frequently document shapes or workload operations are selected:

- **Document shapes**: A weight of 70 means 70% of generated documents will use that shape
- **Workloads**: A weight of 30 means 30% of operations will use that workload

### Max Count

The `max_count` parameter limits how many documents of a specific shape will be generated:

```python
@document_shape(weight=50, max_count=1000)
def generate_rare_item(self, ctx):
    # Only 1000 of these will be generated
    return {"type": "rare", "value": 9999}
```

## Architecture

### Key Components

- **[perf_test_context.py](src/perf_test_context.py)** - Manages shared state across processes using shared memory
- **[decorators.py](src/decorators.py)** - Provides decorator functions for test definition
- **[value_range/](src/value_range/)** - Value generation utilities for deterministic data

### Thread Safety

The framework uses:
- Shared memory for multi-process coordination
- Semaphores for atomic operations
- Atomic counters for document numbering

## Documentation

Additional documentation is available in the [docs/](docs/) directory:

- [Conceptual Overview](docs/conceptual_overview.md)

## Contributing

Contributions are welcome! Please submit pull requests or open issues for improvements, bug fixes, or feature requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
