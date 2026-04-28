# Usage Guide

This document covers the major classes, decorators, and value range types provided by the framework. For the design philosophy behind these, see the [Conceptual Overview](conceptual_overview.md).


# PerfTestUser

`PerfTestUser` is the base class for all performance tests. It extends Locust's `User` and manages the MongoDB connection, test phase lifecycle, document generation, and workload execution.

Subclasses should set `abstract = False` and define at least one `@document_shape` method and one `@workload` method.

```python
from perf_test_user import PerfTestUser, document_shape, workload, pre_load, post_load

class MyWorkload(PerfTestUser):
    abstract = False

    @document_shape(weight=1)
    def my_doc(self, ctx):
        return {
            "_id": ctx.document_number,
            "value": ctx.document_number * 10,
        }

    @workload(weight=1, name="read")
    def read_doc(self):
        self.collection.find_one({"_id": 0})
```

## Instance Attributes

On startup, the framework connects to MongoDB and exposes these on the user instance:

- `self.client` — the `MongoClient` instance
- `self.db` — the database object
- `self.collection` — the collection object
- `self.ctx` — the shared `PerfTestContext` (for accessing document counters, etc.)
- `self.locust_user_id` — a unique integer ID assigned to this user (0-indexed)
- `self.document_count` — total number of documents to generate (from `--document-count`)

## Leader

The user with `locust_user_id == 0` is the leader. The leader is responsible for executing `@pre_load` and `@post_load` methods. All other users idle during those phases. You can check leadership with `self.is_leader()`.

## Command-Line Arguments

The framework registers these arguments with Locust's CLI parser:

| Argument | Default | Description |
|---|---|---|
| `--uri` | `mongodb://localhost:27017` | MongoDB connection URI |
| `--database` | `test` | Database name |
| `--collection` | `test_collection` | Collection name |
| `--document-count` | None | Total documents to generate |
| `--load-batch-size` | `10` | Documents per insert batch during data load |
| `--skip-data-load` | `False` | Skip data loading unconditionally |

The framework also automatically skips data loading if the collection already has at least 95% of the expected document count.


# Decorators

The framework uses decorators to mark methods for specific roles. All decorated methods are discovered automatically at initialization.

## @document_shape(weight, max_count)

Marks a method as a document shape generator. The method receives a `DataGenerationContext` and returns a document dict.

```python
@document_shape(weight=70)
def simple_doc(self, ctx):
    return {"_id": ctx.document_number, "type": "simple"}

@document_shape(weight=30, max_count=3000)
def complex_doc(self, ctx):
    return {"_id": ctx.document_number, "type": "complex", "data": {...}}
```

**Parameters:**
- `weight` (int, default 1) — relative proportion of documents that use this shape. With two shapes at weights 70 and 30, roughly 70% of documents use the first shape.
- `max_count` (int, optional) — hard cap on the number of documents for this shape. If omitted, calculated automatically from the weight and the global `document_count`.

**Context object (`ctx`):**
- `ctx.document_number` — the global document number (unique across all shapes)
- `ctx.shape_ordinal` — the ordinal within this specific shape (0-indexed, increments only for documents of this shape)
- `ctx.shape_max_count` — the max_count for this shape
- `ctx.locust_user_id` — the ID of the user generating this document

The `shape_ordinal` is what you typically pass to ValueRange instances, since it counts from 0 to `shape_max_count - 1` for each shape independently.

## @workload(weight, name)

Marks a method as a workload operation. Each user is assigned exactly one workload for the entire test run.

```python
@workload(weight=80, name="point_read")
def read_by_id(self):
    doc_id = random.randint(0, self.document_count - 1)
    self.collection.find_one({"_id": doc_id})

@workload(weight=20, name="update")
def update_doc(self):
    self.collection.update_one({"_id": 0}, {"$set": {"v": 1}})
```

**Parameters:**
- `weight` (int, default 1) — relative proportion of users assigned to this workload. With 100 users and weights 80/20, approximately 80 users run the first workload and 20 run the second.
- `name` (str, optional) — name used in Locust metrics reporting. Defaults to the method name.

Assignment is deterministic: given the same number of users and the same weights, the same user IDs always get the same workloads.

## @pre_load / @post_load

Mark methods to run before or after data loading. Only the leader executes these. Multiple methods with the same decorator are executed in sequence.

```python
@pre_load
def drop_collection(self):
    self.collection.drop()

@post_load
def create_indexes(self):
    self.collection.create_index([("price", 1)])
```

Note: these decorators take no arguments and are applied directly (no parentheses).


# ValueRange Types

The scalar ValueRange types (`IntegerRange`, `LongRange`, `FloatRange`, `FixedLengthStringRange`) share a common interface for querying generated data during the workload phase. These methods operate on the logical ordinal space regardless of insertion order:

- `random()` — returns a random value that exists in the generated dataset
- `get(ordinal)` — returns the value for a specific ordinal
- `get_percentile(p)` — returns `(value, ordinal)` at percentile `p` (0.0–100.0)
- `random_range(min_p, max_p)` — returns `(value, ordinal)` for a random ordinal within a percentile range

These are the primary tools for building workload queries with predictable selectivity.

`NumericArrayRange` provides its own query methods that operate on distinct *element* values rather than documents. See the NumericArrayRange section below for details.

## InsertionOrder

The scalar ValueRange types accept an `insertion_order` parameter that controls the order in which values are assigned during data loading:

- `InsertionOrder.ASCENDING` (default for scalar types) — ordinals map directly to values in ascending order
- `InsertionOrder.DESCENDING` — ordinals are reversed, so the first document gets the highest value
- `InsertionOrder.RANDOM` — ordinals are shuffled via a Feistel permutation, producing a pseudorandom insertion pattern

`NumericArrayRange` only supports `InsertionOrder.RANDOM` (and uses it as the default).

The insertion order only affects `allocate()` (used during data loading). Query methods like `get()`, `get_percentile()`, and `random()` always operate on the logical ascending order.

## IntegerRange

Generates integers in `[min_value, max_value]`.

```python
from value_range import IntegerRange, InsertionOrder

# All unique values, spanning the full range
price = IntegerRange(0, 100000)

# Each value repeats 5 times (controls selectivity)
category_id = IntegerRange(0, 999, frequency=5)

# Explicit step between values
score = IntegerRange(0, 1000, step_size=10)

# Random insertion order for B-tree testing
key = IntegerRange(0, 999999, insertion_order=InsertionOrder.RANDOM)
```

**Parameters:**
- `min_value` (int, default 0) — inclusive lower bound
- `max_value` (int, default 100) — inclusive upper bound
- `frequency` (int, optional) — how many times each distinct value repeats. Default is 1 (all unique). Higher values mean fewer distinct values and more repetition.
- `step_size` (int, optional) — distance between consecutive distinct values. If omitted, computed automatically to span the full range.
- `insertion_order` — see InsertionOrder above

When neither `frequency` nor `step_size` is provided, the range defaults to all-unique values with `step_size` computed to evenly span `[min_value, max_value]` across the number of documents.

## LongRange

Identical to `IntegerRange` but defaults to the 64-bit signed integer range (`0` to `2^63 - 1`). Exists for MongoDB BSON Int64 compatibility.

```python
from value_range import LongRange

big_id = LongRange(0, 10**15, frequency=1)
```

## FloatRange

Generates floating-point values in `[min_value, max_value]`.

```python
from value_range import FloatRange

temperature = FloatRange(0.0, 100.0)
price = FloatRange(9.99, 999.99, frequency=10)
```

Same parameters as `IntegerRange`, but `min_value`, `max_value`, and `step_size` are floats.

## FixedLengthStringRange

Generates fixed-length strings by treating the ordinal as a base-N number where N is the alphabet size.

```python
from value_range import FixedLengthStringRange

# 3-character strings from a-zA-Z: "aaa", "aab", "aac", ...
sku = FixedLengthStringRange(length=3)

# Binary strings: "000", "001", "010", "011", ...
code = FixedLengthStringRange(length=3, alphabet="01")
```

**Parameters:**
- `length` (int) — length of each generated string
- `alphabet` (str, optional) — characters to use. Default is `string.ascii_letters` (a-zA-Z).
- `insertion_order` — see InsertionOrder above

The total number of distinct strings is `len(alphabet) ** length`. Ordinals beyond this wrap around.

## NumericArrayRange

Generates arrays of integers with controllable per-element selectivity. Designed for testing multikey indexes.

```python
from value_range import NumericArrayRange

# 3-element arrays, each element value globally unique
tags = NumericArrayRange(0, 299999, array_size=3, frequency=1)
tags.set_max_count(100000)

# Each distinct value appears in ~100 element slots
categories = NumericArrayRange(0, 2999, array_size=3, frequency=100)
categories.set_max_count(100000)
```

**Parameters:**
- `min_value` (int, default 0) — inclusive lower bound for element values
- `max_value` (int, default 100) — inclusive upper bound for element values
- `array_size` (int, default 3) — number of elements per array
- `frequency` (int, optional) — how many `(doc, position)` slots share each distinct value. Controls selectivity. Default is 1.
- `step_size` (int, optional) — distance between consecutive distinct values
- `insertion_order` — only `InsertionOrder.RANDOM` is currently supported (and is the default)

Elements within a single array are always unique. The `frequency` parameter controls global selectivity: with `frequency=100` and `array_size=3`, each distinct value appears in roughly 100 element slots across all documents.

**Additional query methods:**

`NumericArrayRange` overrides the shared query interface to operate on distinct *element* values rather than documents:

- `random()` — returns a random element value (`int`), not a list
- `get_percentile(p)` — returns `(value, ordinal)` based on distinct element values in ascending order
- `random_range(min_p, max_p)` — returns `(value, ordinal)` for a random element value within a percentile range
- `get_array(doc_id)` — returns the full array for a document
- `get_element(doc_id, position)` — returns a single element value at a specific position
- `describe()` — prints a human-readable summary of the computed configuration


# Using ValueRanges in Document Shapes

ValueRange instances are typically defined as class attributes on the `PerfTestUser` subclass and referenced in document shape methods. The framework automatically replaces them with generated values during data loading.

```python
class MyWorkload(PerfTestUser):
    abstract = False

    price = IntegerRange(100, 10000, insertion_order=InsertionOrder.RANDOM)
    category = IntegerRange(0, 49, frequency=200)

    @document_shape(weight=1)
    def product(self, ctx):
        return {
            "_id": ctx.document_number,
            "price": self.price,       # replaced with an integer at insert time
            "category": self.category,  # replaced with an integer at insert time
        }

    @workload(weight=1, name="by_category")
    def query_by_category(self):
        val = self.category.random()
        self.collection.find({"category": val}).limit(10)
```

**Important constraints:**
- A ValueRange instance must belong to exactly one document shape. If you have multiple shapes, create separate ValueRange instances for each.
- The framework automatically sets `max_count` on each ValueRange based on the shape's document count. You don't need to call `set_max_count()` manually in most cases.
- During the workload phase, use `random()`, `get()`, `get_percentile()`, and `random_range()` to generate query parameters that are guaranteed to match documents in the dataset.
