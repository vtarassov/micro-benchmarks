# Conceptual Overview

This document provides a high-level overview of the core concepts and execution model of the performance testing framework.

This framework aims to solve several problems: 
1. Separating the test into discrete phases; one for preparing a dataset and another for the actual workload
2. Providing flexible and intuitive mechanisms to specify the dataset and workload
3. Allowing users to write queries with predictable results, even for a randomly-generated dataset


# PerfTestUser

The `PerfTestUser` class is the core abstraction for defining performance tests. It extends Locust's `User` class and encapsulates all functionality required by the framework.

Its implementation strives to follow Locust's general design principles and be inter-operable with other Users, as needed. 

# Workload Phases

A performance test is logically broken down into phases:

**Pre-Load**
: An optional phase that executes before any data is loaded into the database. Can be used to explicitly create collections or indexes.

**Data Load**
: This phase populates the database with the initial dataset required for testing.

**Post-Load**
: An optional phase that executes after data loading completes but before the main workload begins. Primary use-case is creating indexes on existing collections.

**Warmup**
: An optional phase at the beginning of the workload where operations are executed but metrics are not collected.

**Workload**
: The main performance testing phase.

These phases must be strictly synchronized to be exclusive to one-another. This is not a concept natively understood by Locust, which heavily relies on stochastic mechanisms. For example, Users are initiated over time and started immediately, meaning a phase might finish before all Users have been created. Likewise, in a configuration includes multiple Users the final distribution of per-User counts is not deterministic since users are selected on-demand at startup.

# Document Generation

The framework needs to understand enough about the structure of generated documents to coordinate data loading and enable deterministic queries, but it should avoid imposing unnecessary constraints on what those documents look like.

To that end, the core modeling concept is the **document shape**: a user-defined function that transforms an integer ordinal into a document. The framework distributes ordinals to workers during data loading, and each worker calls the appropriate shape function to produce the document for that ordinal. The key requirement is that this transformation is fully repeatable — given the same ordinal (and random seed, if applicable), the shape function must always produce the same document. This is what makes the generated dataset queryable in a predictable way; truly random data is extremely difficult to write targeted queries against.

A test can define multiple document shapes, each with a relative weight. The framework uses a deterministic algorithm to assign shapes to ordinals, so all workers agree on which shape produced which document without any communication.

# ValueRange

Within a document shape, a `ValueRange` handles the mapping from an ordinal to an individual field value. The shape function itself defines the document structure, but delegates the actual value generation for each field to ValueRange instances.

A ValueRange creates an order-preserving mapping from ordinals [0..N) into the desired value space. Because the mapping is order-preserving, a query can use ordinals to select values that are guaranteed to exist in the dataset, or to specify the bounds of a range query with a known result set size.

The ordinals passed to a ValueRange don't have to arrive in ascending order. The insertion order can be controlled independently, which is useful for reaching a desired insertion pattern (e.g., random order to simulate a realistic write workload against a B-tree index).

Order-preserving mappings aren't practical in all cases — generating natural text, for example. But even then, the ordinal-based strategy can still be used to look up specific values that are known to exist in the dataset.

# Locust Integration

Locust is a flexible framework that handles a lot of the heavy lifting — process management, user spawning, metrics collection, and reporting — but its modeling doesn't align fully with this framework's use-case.

Organizing a single run into sequential phases is not something Locust supports natively. Currently, phase synchronization is managed using OS-level primitives (shared memory and semaphores). Eventually, the goal is to transition to using Locust's built-in message model for this coordination.

Likewise, Locust's stochastic selection model for users and workload weights makes it difficult to generate consistent datasets with the level of accuracy that the framework needs. Both document shape assignment and workload distribution are handled deterministically by the framework rather than relying on Locust's weighted random selection.
