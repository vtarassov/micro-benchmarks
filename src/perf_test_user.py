from typing import Dict, Any, Optional, Callable, List, Tuple
import time
import random
import math

from locust import User, task, events
from locust.exception import StopUser
from locust.runners import WorkerRunner, MasterRunner
from pymongo import MongoClient
from pymongo.errors import PyMongoError


try:
    from .decorators import document_shape, workload, pre_load, post_load
    from .perf_test_context import PerfTestContext, DataGenerationContext, TestPhase
    from .value_range import process_document_shape
except ImportError:
    # Support direct imports when not used as a package
    from decorators import document_shape, workload, pre_load, post_load
    from perf_test_context import PerfTestContext, DataGenerationContext, TestPhase
    from value_range import process_document_shape

# Re-export decorator for convenient imports
__all__ = ['PerfTestUser', 'document_shape', 'workload', 'pre_load', 'post_load', 'get_parsed_args', 'PerfTestContext', 'DataGenerationContext', 'TestPhase']


# Storage for parsed arguments - will be populated by Locust
_parsed_args = None

# Global context instance shared across all workers
_perf_test_context = None


def init_context(environment=None) -> PerfTestContext:
    """Initialize the shared PerfTestContext instance.

    Args:
        environment: Locust environment (used to determine create vs attach mode)
    """
    global _perf_test_context

    if _perf_test_context is not None:
        return _perf_test_context

    is_master = True

    if environment and hasattr(environment, 'runner'):
        if isinstance(environment.runner, WorkerRunner):
            # Worker in distributed mode: attach to master's context
            is_master = False
        elif isinstance(environment.runner, MasterRunner):
            # Master in distributed mode: create the context
            is_master = True
        else:
            # Standalone mode (LocalRunner): create the context
            is_master = True

    _perf_test_context = PerfTestContext(is_master=is_master)
    return _perf_test_context


def get_context() -> Optional[PerfTestContext]:
    """Get the shared PerfTestContext instance (must be initialized first).

    Returns:
        PerfTestContext instance or None if not initialized
    """
    return _perf_test_context


@events.init_command_line_parser.add_listener
def _setup_command_line_parser(parser):
    # Add standard framework arguments
    parser.add_argument(
        '--uri',
        type=str,
        default='mongodb://localhost:27017',
        help='MongoDB connection URI (default: mongodb://localhost:27017)'
    )

    parser.add_argument(
        '--database',
        type=str,
        default='test',
        help='Database name (default: test)'
    )

    parser.add_argument(
        '--collection',
        type=str,
        default='test_collection',
        help='Collection name (default: test_collection)'
    )

    parser.add_argument(
        '--document-count',
        type=int,
        default=None,
        help='Maximum number of documents to insert (default: unlimited)'
    )

    parser.add_argument(
        '--load-batch-size',
        type=int,
        default=10,
        help='Number of documents to generate and insert in a single batch during data load (default: 10)'
    )

    parser.add_argument(
        '--skip-data-load',
        action='store_true',
        default=False,
        help='Force skip data loading regardless of collection state. '
             'Takes priority over automatic estimated_document_count detection.'
    )


@events.test_start.add_listener
def _on_test_start(environment, **kwargs):
    global _parsed_args
    _parsed_args = environment.parsed_options

    # Initialize the shared context at test start
    init_context(environment=environment)


@events.quitting.add_listener
def _on_quit(environment, **kwargs):
    """Cleanup on process exit to prevent shutdown conflicts with gevent."""
    import pymongo

    # Force immediate shutdown of all pymongo periodic executors
    # This prevents the atexit handler from trying to join threads during gevent shutdown
    try:
        pymongo.synchronous.monitor._shutdown_resources()
    except Exception:
        pass  # Ignore errors during shutdown

    global _perf_test_context
    if _perf_test_context is not None:
        if _perf_test_context._is_master:
            _perf_test_context.teardown()
        else:
            _perf_test_context.cleanup()
        _perf_test_context = None


def get_parsed_args():
    return _parsed_args


class PerfTestUser(User):
    # Configuration attributes (must be set in subclasses)
    conn_string: str = "mongodb://localhost:27017"
    db_name: str = "test"
    collection_name: str = "test_collection"
    batch_size: int = 1000
    total_documents: int = 10000
    document_count: Optional[int] = None
    load_batch_size: int = 1

    # Set to True so this user doesn't start automatically
    abstract = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client: Optional[MongoClient] = None
        self.db = None
        self.collection = None
        self._document_shapes: List[Tuple[Callable, int, Optional[int]]] = []  # (method, weight, max_count)
        self.candidate_document_shapes: List[Callable] = []
        self._workloads: List[Tuple[Callable, int, str]] = []
        self.candidate_workloads: List[Callable] = []
        self._pre_load_methods: List[Callable] = []
        self._post_load_methods: List[Callable] = []
        self.ctx: Optional[PerfTestContext] = None
        self.locust_user_id: Optional[int] = None
        self.data_load_complete: bool = False
        self._assigned_workload: Optional[Tuple[Callable, str]] = None
        # Document shape tracking for deterministic mapping
        self._last_document_number: int = -1
        self._shape_ordinal_counters: Dict[int, int] = {}  # shape_index -> count
        # Phase caching to avoid unnecessary shared memory access
        self._cached_phase: Optional[TestPhase] = None
        # Skip data load when collection already has sufficient data
        # (set in on_start() after connecting to DocumentDB)
        self._skip_data_load: bool = False
        self._collect_decorated_methods()

    def is_leader(self) -> bool:
        """Check if this user is the leader (locust_user_id == 0)."""
        return self.locust_user_id == 0

    def mark_data_load_complete(self) -> None:
        """
        Mark this user as having completed data loading.

        This method can be called by subclasses to indicate that this user
        has finished loading data and should wait for other users to complete.

        Decrements the active data loaders counter. When the counter reaches
        zero, the phase automatically transitions to POST_LOAD.
        """
        if not self.data_load_complete:
            self.data_load_complete = True
            # Decrement the active data loaders counter
            self.ctx.decrement_active_data_loaders()

    def _collect_decorated_methods(self):
        """Collect document_shape, workload, pre_load, and post_load decorated methods."""
        # Iterate through the class (not instance) to avoid triggering properties
        for attr_name in dir(self.__class__):
            if attr_name.startswith('_'):
                continue
            try:
                # Get from class first to check for decorator
                class_attr = getattr(self.__class__, attr_name)
                if callable(class_attr):
                    # Check for document_shape decorator
                    if hasattr(class_attr, '_document_shape_weight'):
                        # Now get the bound method from instance
                        bound_method = getattr(self, attr_name)
                        weight = class_attr._document_shape_weight
                        max_count = getattr(class_attr, '_document_shape_max_count', None)
                        self._document_shapes.append((bound_method, weight, max_count))

                    # Check for workload decorator
                    if hasattr(class_attr, '_workload_weight'):
                        # Now get the bound method from instance
                        bound_method = getattr(self, attr_name)
                        weight = class_attr._workload_weight
                        name = class_attr._workload_name
                        self._workloads.append((bound_method, weight, name))

                    # Check for pre_load decorator
                    if hasattr(class_attr, '_is_pre_load'):
                        bound_method = getattr(self, attr_name)
                        self._pre_load_methods.append(bound_method)

                    # Check for post_load decorator
                    if hasattr(class_attr, '_is_post_load'):
                        bound_method = getattr(self, attr_name)
                        self._post_load_methods.append(bound_method)
            except AttributeError:
                # Skip attributes that can't be accessed during init
                continue

    def _calculate_max_counts(self):
        """
        Calculate max_count for document shapes where it wasn't explicitly provided.

        First subtracts all explicitly specified max_counts from the global document_count,
        then distributes the remaining count among shapes without max_count based on their
        weights, rounded up.
        """
        if not self._document_shapes:
            return

        if self.document_count is None:
            return

        # First pass: subtract all specified max_counts from the global total
        # and calculate total weight of shapes without explicit max_count
        remaining_documents = self.document_count
        weight_without_max_count = 0

        for method, weight, max_count in self._document_shapes:
            if max_count is not None:
                remaining_documents -= max_count
            else:
                weight_without_max_count += weight

        # Second pass: calculate max_counts for shapes without explicit values
        updated_shapes = []
        for method, weight, max_count in self._document_shapes:
            if max_count is None:
                if weight_without_max_count > 0:
                    # Calculate max_count based on weight proportion of remaining documents
                    calculated_max_count = math.ceil((weight / weight_without_max_count) * remaining_documents)
                    updated_shapes.append((method, weight, calculated_max_count))
                else:
                    # No remaining weight, set to 0
                    updated_shapes.append((method, weight, 0))
            else:
                updated_shapes.append((method, weight, max_count))

        self._document_shapes = updated_shapes

    def get_shape_for_document_number(self, document_number: int) -> Tuple[Callable, int, Optional[int]]:
        """
        Get the document shape, ordinal, and max_count for a given document number.

        This method uses a deterministic round-robin algorithm (similar to _choose_workload)
        to ensure all users across all processes agree on which shape to use for a given
        document_number and what the ordinal within that shape should be.

        The method maintains internal counters and incrementally calculates from the last
        seen document number to avoid keeping the entire sequence in memory.

        Args:
            document_number: Global document number to get shape for

        Returns:
            Tuple[Callable, int, Optional[int]]: A tuple of (shape_method, ordinal_within_shape, max_count)
        """
        if not self._document_shapes:
            # Fallback to document_shape() method if no decorated methods
            # In this case, all documents are the same shape
            return (self.document_shape, document_number, None)

        # If there's only one shape, return it with document_number as ordinal
        if len(self._document_shapes) == 1:
            return (self._document_shapes[0][0], document_number, self._document_shapes[0][2])

        # Initialize counters if this is the first call
        if not self._shape_ordinal_counters:
            for idx in range(len(self._document_shapes)):
                self._shape_ordinal_counters[idx] = 0

        # Calculate shapes from last_document_number + 1 to document_number
        start = self._last_document_number + 1

        for doc_num in range(start, document_number + 1):
            # Find shape with lowest (current_count / desired_weight) ratio
            # that hasn't reached its max_count
            min_ratio = float('inf')
            selected_shape_idx = 0

            for idx, (method, weight, max_count) in enumerate(self._document_shapes):
                # Skip shapes that have reached their max_count
                if max_count is not None and self._shape_ordinal_counters[idx] >= max_count:
                    continue

                ratio = self._shape_ordinal_counters[idx] / weight
                if ratio < min_ratio:
                    min_ratio = ratio
                    selected_shape_idx = idx

            # Increment the counter for the selected shape
            self._shape_ordinal_counters[selected_shape_idx] += 1

            # If this is the document we're looking for, save the result
            if doc_num == document_number:
                shape_method = self._document_shapes[selected_shape_idx][0]
                shape_max_count = self._document_shapes[selected_shape_idx][2]
                # Ordinal is the count - 1 (since we already incremented)
                ordinal = self._shape_ordinal_counters[selected_shape_idx] - 1

        # Update last seen document number
        self._last_document_number = document_number

        return (shape_method, ordinal, shape_max_count)

    def _choose_document_shape(self) -> Callable:
        if not self._document_shapes:
            # Fallback to document_shape() method if no decorated methods
            return self.document_shape

        # If there's only one shape, return it immediately
        if len(self._document_shapes) == 1:
            return self._document_shapes[0][0]

        # If candidate list is empty, rebuild and shuffle it
        if not self.candidate_document_shapes:
            # Flatten: add each method 'weight' times
            for method, weight, max_count in self._document_shapes:
                self.candidate_document_shapes.extend([method] * weight)
            # Shuffle for randomness
            random.shuffle(self.candidate_document_shapes)

        # Pop the first element (deterministic selection with strict proportions)
        return self.candidate_document_shapes.pop(0)

    def _choose_workload(self) -> Optional[Tuple[Callable, str]]:
        """
        Choose a workload method based on locust_user_id and weights.

        The first N workers (where N = number of workload functions) get one unique
        workload each. Workers beyond N are assigned by maintaining the desired
        proportion ratios - always selecting the workload that is furthest below
        its target proportion.

        The result is cached after the first calculation.

        Returns:
            Optional[Tuple[Callable, str]]: A tuple of (method, name) or None if no workloads
        """
        # Return cached result if already calculated
        if self._assigned_workload is not None:
            return self._assigned_workload

        if not self._workloads:
            # No workload methods defined
            return None

        # If there's only one workload, cache and return it
        if len(self._workloads) == 1:
            method, _, name = self._workloads[0]
            self._assigned_workload = (method, name)
            return self._assigned_workload

        num_workloads = len(self._workloads)

        # First N workers get one unique workload each
        if self.locust_user_id < num_workloads:
            method, _, name = self._workloads[self.locust_user_id]
            self._assigned_workload = (method, name)
            return self._assigned_workload

        # Workers beyond N: calculate assignment by maintaining proportions
        # Track current counts for each workload (starting with the first N workers)
        counts = [1] * num_workloads  # Each of first N workers got one workload

        # Calculate assignments for workers from num_workloads up to this locust_user_id
        for worker_id in range(num_workloads, self.locust_user_id + 1):
            # Find workload with lowest (current_count / desired_weight) ratio
            min_ratio = float('inf')
            selected_workload_idx = 0

            for idx, (method, weight, name) in enumerate(self._workloads):
                ratio = counts[idx] / weight
                if ratio < min_ratio:
                    min_ratio = ratio
                    selected_workload_idx = idx

            # If this is our worker, cache and return the selected workload
            if worker_id == self.locust_user_id:
                method, _, name = self._workloads[selected_workload_idx]
                self._assigned_workload = (method, name)
                return self._assigned_workload

            # Otherwise, just update counts and continue
            counts[selected_workload_idx] += 1

        # Should never reach here, but return None for safety
        return None

    def on_start(self):
        # Get the shared context (already initialized at test start)
        self.ctx = _perf_test_context

        # Register this locust user and get a unique ID
        self.locust_user_id = self.ctx.register_locust_user()

        # Apply command-line arguments if available
        args = get_parsed_args()
        if args:
            if hasattr(args, 'uri'):
                self.conn_string = args.uri
            if hasattr(args, 'database'):
                self.db_name = args.database
            if hasattr(args, 'collection'):
                self.collection_name = args.collection
            if hasattr(args, 'document_count') and args.document_count is not None:
                self.document_count = args.document_count
            if hasattr(args, 'load_batch_size'):
                self.load_batch_size = args.load_batch_size

        self._calculate_max_counts()

        # Configure MongoClient to work better with gevent
        # connect=False delays connection until first operation
        # serverSelectionTimeoutMS prevents long hangs during shutdown
        self.client = MongoClient(
            self.conn_string,
            connect=False,
            serverSelectionTimeoutMS=60000,
            socketTimeoutMS=60000,
            connectTimeoutMS=60000
        )
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

        # Configure ValueRange instances for all document shapes via dry-run.
        # This ensures they are ready even when data loading is skipped.
        self._configure_value_ranges()

        # Smart skip detection: if the collection already has enough documents,
        # skip the data load phase. Each user runs this independently so both
        # leader and non-leader users agree on the skip decision.
        self._detect_skip_data_load()

    def _configure_value_ranges(self) -> None:
        """
        Dry-run each document shape to configure ValueRange instances.

        Invokes each shape method with a minimal DataGenerationContext and passes
        the resulting doc through process_document_shape(dry_run=True). This
        sets max_count and _owning_shape on all ValueRange instances without
        allocating values or inserting documents.

        Required so that workload methods (random(), get_percentile(), etc.) can
        use ValueRanges even when data loading is skipped entirely.

        Assumptions:
            Each field is bound to exactly one ValueRange instance (1:1 mapping).
            The framework does not support the same field randomly picking
            different ValueRange instances across different documents.

        Raises:
            ValueError: If document_count is None but shapes exist. ValueRange
                        configuration requires a document count.
        """
        if not self._document_shapes:
            return

        for shape_method, _weight, shape_max_count in self._document_shapes:
            if shape_max_count is None:
                continue

            if self.document_count is None:
                raise ValueError(
                    "document_count is required to configure ValueRanges. "
                    "Pass --document-count on the command line."
                )

            # Build a minimal DataGenerationContext so the shape method can run
            data_ctx = DataGenerationContext()
            data_ctx.document_number = 0
            data_ctx.shape_ordinal = 0
            data_ctx.shape_max_count = shape_max_count
            data_ctx.locust_user_id = self.locust_user_id

            # Invoke shape method to get a doc containing ValueRange instances
            doc = shape_method(data_ctx)

            # Dry-run: sets max_count and _owning_shape on ValueRanges but does
            # not call allocate() or replace values in the doc.
            process_document_shape(
                doc,
                ordinal=0,
                max_count=shape_max_count,
                shape_id=shape_method.__name__,
                dry_run=True,
            )

    def _detect_skip_data_load(self) -> None:
        """
        Decide whether to skip the data load phase.

        Priority order:
        1. ``--skip-data-load`` CLI flag — if passed, skip unconditionally.
        2. Automatic detection — if ``estimated_document_count()`` >=
           ``document_count * 0.95`` (95 % tolerance to account for the
           approximate nature of ``estimated_document_count``), skip.

        Each user runs this independently (not just the leader) so all users
        agree on the skip decision without needing to broadcast shared state.
        """
        # 1. CLI override takes priority
        args = get_parsed_args()
        if args and getattr(args, 'skip_data_load', False):
            self._skip_data_load = True
            if self.is_leader():
                print("[SKIP_DATA_LOAD] --skip-data-load flag set. Skipping data load.")
            return

        # 2. Automatic detection with 0.95 tolerance
        if self.document_count is None:
            return

        try:
            existing_count = self.collection.estimated_document_count()
        except Exception as e:
            # Collection might not exist yet; just load normally
            if self.is_leader():
                print(
                    f"[SKIP_DATA_LOAD] Could not check collection: {e}. "
                    f"Will load data."
                )
            return

        threshold = self.document_count * 0.95
        if existing_count >= threshold:
            self._skip_data_load = True
            if self.is_leader():
                print(
                    f"[SKIP_DATA_LOAD] Collection has {existing_count:,} docs "
                    f"(>= {threshold:,.0f}, 95% of {self.document_count:,} expected). "
                    f"Skipping data load."
                )

    def on_stop(self):
        """Close MongoDB client connection when user stops."""
        if self.client:
            try:
                self.client.close()
            except Exception:
                # Ignore errors during cleanup
                pass

    def document_shape(self, ctx: Optional[DataGenerationContext] = None) -> Dict[str, Any]:
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement document_shape() method"
        )

    def _generate_document(self, document_number: int) -> Dict[str, Any]:
        """
        Generate a single document for the given document number.

        Args:
            document_number: The global document number to generate

        Returns:
            The generated and processed document ready for insertion
        """
        # Create data generation context
        data_ctx = DataGenerationContext()
        data_ctx.document_number = document_number
        data_ctx.locust_user_id = self.locust_user_id

        # Get the document shape and ordinal using deterministic mapping
        shape_method, shape_ordinal, shape_max_count = self.get_shape_for_document_number(document_number)
        data_ctx.shape_ordinal = shape_ordinal
        data_ctx.shape_max_count = shape_max_count

        # Generate the document using the shape method
        document = shape_method(data_ctx)

        # Process document to replace ValueRange instances with actual values
        # This calls allocate() on them and sets max_count if needed
        # Tracks shape ownership to prevent ValueRanges from being shared across shapes
        document = process_document_shape(
            document,
            ordinal=shape_ordinal,
            max_count=shape_max_count,
            shape_id=shape_method.__name__
        )

        return document

    @task
    def run_workload(self):
        start_time = time.perf_counter()
        exception = None
        response_length = 0
        operation_name = None

        try:
            # Check current phase with caching optimization
            # Once in WORKLOAD phase, stay cached there
            # During DATA_LOAD, only check shared memory if data load is complete
            if self._cached_phase == TestPhase.WORKLOAD:
                current_phase = TestPhase.WORKLOAD
            elif self._cached_phase == TestPhase.DATA_LOAD and not self.data_load_complete:
                current_phase = TestPhase.DATA_LOAD
            else:
                # Need to check shared memory
                current_phase = self.ctx.get_current_phase()
                self._cached_phase = current_phase

            if current_phase == TestPhase.PRE_LOAD:
                # PRE_LOAD phase: Leader executes pre_load methods and transitions to DATA_LOAD
                if self.is_leader():
                    print("Running Pre Load")
                    operation_name = "pre_load"
                    # Execute all pre_load methods in sequence
                    for pre_load_method in self._pre_load_methods:
                        pre_load_method()
                    # Leader transitions to DATA_LOAD phase
                    self.ctx.set_current_phase(TestPhase.DATA_LOAD)
                else:
                    # Non-leaders sleep briefly
                    time.sleep(0.1)

            elif current_phase == TestPhase.DATA_LOAD:
                # Data loading phase
                # Check if this user has already completed data loading
                if self.data_load_complete:
                    # Already done loading, sleep briefly
                    time.sleep(0.1)
                    return

                # Explicit skip: collection already has enough data
                if self._skip_data_load:
                    self.mark_data_load_complete()
                    return

                operation_name = "data_load"

                # Batch insert logic
                if self.load_batch_size > 1:
                    # Get a range of document numbers in a single atomic operation
                    start_doc_num, end_doc_num = self.ctx.next_document_numbers(self.load_batch_size)

                    documents_to_insert = []

                    for document_number in range(start_doc_num, end_doc_num):
                        # Check if we've exceeded the document count
                        if self.document_count is not None and document_number >= self.document_count:
                            # Mark this user as complete
                            self.mark_data_load_complete()
                            break

                        # Generate the document
                        document = self._generate_document(document_number)
                        documents_to_insert.append(document)

                    # Insert all documents at once if we have any
                    if documents_to_insert:
                        self.collection.insert_many(documents_to_insert)
                        response_length = len(documents_to_insert)
                    else:
                        response_length = 0
                else:
                    # Single document insert (original logic)
                    # Get next document number
                    document_number = self.ctx.next_document_number()

                    # Check if we've exceeded the document count
                    if self.document_count is not None and document_number >= self.document_count:
                        # Mark this user as complete
                        self.mark_data_load_complete()
                        return

                    # Generate the document
                    document = self._generate_document(document_number)

                    # Insert the document into the collection
                    self.collection.insert_one(document)
                    response_length = 1

            elif current_phase == TestPhase.POST_LOAD:
                # POST_LOAD phase: Leader executes post_load methods and transitions to WORKLOAD
                if self.is_leader():
                    print("Running Post Load")
                    operation_name = "post_load"
                    # Execute all post_load methods in sequence
                    for post_load_method in self._post_load_methods:
                        post_load_method()
                    
                    self.ctx.set_current_phase(TestPhase.WORKLOAD)
                else:
                    # Non-leaders sleep briefly
                    time.sleep(0.1)
            
            elif current_phase == TestPhase.WORKLOAD:
                # Run workload phase
                workload_result = self._choose_workload()
                if workload_result is not None:
                    workload_method, operation_name = workload_result
                    # Execute the workload method
                    workload_method()
                else:
                    # No workloads defined, use generic operation name
                    operation_name = "workload"

        except PyMongoError as e:
            exception = e
        except Exception as e:
            exception = e
        finally:
            # Record metrics in microseconds
            response_time = int((time.perf_counter() - start_time) * 1000000)

            if operation_name is not None:
                events.request.fire(
                    request_type="MONGODB",
                    name=operation_name,
                    response_time=response_time,
                    response_length=1,
                    exception=exception,
                    context={}
                )
