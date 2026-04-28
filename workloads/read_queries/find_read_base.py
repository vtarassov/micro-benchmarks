"""
Find/Read Testing Base Schema — Controllable selectivity via IntegerRange + NumericArrayRange.

Schema:
  10 scalar fields with varying selectivity (1 to 5000 doc/val)
  10 array fields (array_size=3) with varying selectivity (1 to 5000 doc/val)
  1 padding field for doc size control

Do not run this file directly — use a subclass workload (point_scalar.py, etc.) or
run this file to load data only.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from perf_test_user import PerfTestUser, document_shape, workload, pre_load, post_load, get_parsed_args
from value_range import IntegerRange, NumericArrayRange, InsertionOrder
from locust import events
from locust.runners import WorkerRunner


# ============================================================================
# Selectivity mapping — shared by all query workloads
# ============================================================================

SELECTIVITY_CHOICES = ["unique", "5", "10", "50", "100", "1k", "2k", "3k", "4k", "5k"]


def parse_selectivity(sel_str):
    """
    Map CLI --selectivity string to (suffix, numeric_value).

    Returns:
        tuple: (suffix, numeric_value)
            suffix: field name suffix, e.g. "unique", "sel5", "sel100", "sel1k"
            numeric_value: integer selectivity, e.g. 1, 5, 100, 1000
    """
    mapping = {
        "unique": ("unique", 1),
        "5": ("sel5", 5),
        "10": ("sel10", 10),
        "50": ("sel50", 50),
        "100": ("sel100", 100),
        "1k": ("sel1k", 1000),
        "2k": ("sel2k", 2000),
        "3k": ("sel3k", 3000),
        "4k": ("sel4k", 4000),
        "5k": ("sel5k", 5000),
    }
    return mapping[sel_str]


# ============================================================================
# Distributed-safe quit: worker sends a message to master, master quits cleanly
# ============================================================================

_QUIT_MESSAGE_NAME = 'find_read_quit'


def _handle_quit_message(environment, msg, **kwargs):
    """Master-side handler: receive quit message from any worker and quit all."""
    print(f"[MASTER] Received {_QUIT_MESSAGE_NAME!r}: {msg.data}. Stopping all workers.")
    environment.runner.quit()


@events.init_command_line_parser.add_listener
def _setup_query_parser(parser):
    """CLI options shared by all query workloads that inherit FindReadWorkload."""
    parser.add_argument('--skip-index-setup', action='store_true', default=False,
                        help='Skip drop_indexes + create_index in @post_load. '
                             'Useful when running multiple query workloads back-to-back '
                             'against the same field (the index is already there).')


@events.init.add_listener
def _register_quit_handler(environment, **kwargs):
    """Register the quit message handler on master (and standalone local runs).

    Workers don't register — they're the senders. Local runs (LocalRunner) also
    register so that local (non-distributed) execution works.
    """
    if not isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message(_QUIT_MESSAGE_NAME, _handle_quit_message)


class FindReadWorkload(PerfTestUser):
    """
    Find/read testing schema — abstract base for all read-query workloads.

    This class is ``abstract = True`` — it is not instantiated directly by
    Locust. Use ``find_read_loader.py`` as a pure data loader (it handles
    the ``noop`` fallback workload and ``quit_after_load`` logic), or
    subclass this class (see ``point_scalar.py``, etc.) to add query workloads.

    Features inherited by subclasses:
    - Schema: 10 scalar + 10 array fields at varying selectivity (auto-configured
      by the framework via dry-run in ``on_start``)
    - Smart-skip of data load: if collection already has enough docs, ``DATA_LOAD``
      phase is skipped automatically (also overridable via ``--skip-data-load``)
    - ``@pre_load`` drops the collection only when actually loading
    - ``@post_load finish_schema_info`` prints schema configuration summary
    """

    abstract = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Read document_count from CLI args directly (available at __init__ time)
        cli_args = get_parsed_args()
        if cli_args and hasattr(cli_args, 'document_count') and cli_args.document_count:
            n = cli_args.document_count
        else:
            n = self.document_count or 100000

        def _safe_freq(freq, total_slots, array_size=1):
            """Cap frequency so num_distinct >= array_size * 10 (avoids infinite rejection loops).
            When document_count is small, high frequency values would produce too few distinct
            values, causing NumericArrayRange.generate() to loop forever trying to avoid
            intra-array duplicates."""
            min_distinct = max(array_size * 10, 10)
            max_freq = max(1, total_slots // min_distinct)
            capped = min(freq, max_freq)
            if capped < freq:
                print(f"[WARN] frequency {freq} capped to {capped} (n={n}, need >= {min_distinct} distinct values)")
            return capped

        # === Scalar fields ===
        # max_count is set automatically by the framework via dry-run in on_start()
        self.scalar_unique = IntegerRange(0, n - 1, frequency=1, insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel5   = IntegerRange(0, n - 1, frequency=_safe_freq(5, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel10  = IntegerRange(0, n - 1, frequency=_safe_freq(10, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel50  = IntegerRange(0, n - 1, frequency=_safe_freq(50, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel100 = IntegerRange(0, n - 1, frequency=_safe_freq(100, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel1k  = IntegerRange(0, n - 1, frequency=_safe_freq(1000, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel2k  = IntegerRange(0, n - 1, frequency=_safe_freq(2000, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel3k  = IntegerRange(0, n - 1, frequency=_safe_freq(3000, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel4k  = IntegerRange(0, n - 1, frequency=_safe_freq(4000, n), insertion_order=InsertionOrder.RANDOM)
        self.scalar_sel5k  = IntegerRange(0, n - 1, frequency=_safe_freq(5000, n), insertion_order=InsertionOrder.RANDOM)

        # === Array fields ===
        # max_value = n * array_size - 1 so global distinct values scale with doc count
        # frequency controls how many element slots share the same value
        # max_count is set automatically by the framework via dry-run in on_start()
        arr_max = n * 3 - 1
        arr_slots = n * 3
        self.arr_unique = NumericArrayRange(0, arr_max, array_size=3, frequency=1)
        self.arr_sel5   = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(5, arr_slots, 3))
        self.arr_sel10  = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(10, arr_slots, 3))
        self.arr_sel50  = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(50, arr_slots, 3))
        self.arr_sel100 = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(100, arr_slots, 3))
        self.arr_sel1k  = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(1000, arr_slots, 3))
        self.arr_sel2k  = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(2000, arr_slots, 3))
        self.arr_sel3k  = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(3000, arr_slots, 3))
        self.arr_sel4k  = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(4000, arr_slots, 3))
        self.arr_sel5k  = NumericArrayRange(0, arr_max, array_size=3, frequency=_safe_freq(5000, arr_slots, 3))

        # Padding (fixed size)
        self._padding = "x" * 100

    @pre_load
    def setup_collection(self):
        """Drop the collection to prepare for clean data load.

        Skipped when the framework's smart-skip detected sufficient existing
        data (_skip_data_load=True) — no need to drop if we're not reloading.
        """
        if self._skip_data_load:
            print(f"[PRE_LOAD] _skip_data_load=True, keeping collection {self.collection_name}")
            return
        print(f"[PRE_LOAD] Dropping collection: {self.collection_name}")
        try:
            self.collection.drop()
            print("[PRE_LOAD] Collection dropped")
        except Exception as e:
            print(f"[PRE_LOAD] Warning: {e}")

    @document_shape(weight=100)
    def find_read_doc(self, ctx=None):
        """Generate a 21-field document. ValueRange instances replaced by process_document_shape."""
        doc_num = ctx.document_number if ctx else 0
        return {
            "_id": doc_num,
            "scalar_unique": self.scalar_unique,
            "scalar_sel5":   self.scalar_sel5,
            "scalar_sel10":  self.scalar_sel10,
            "scalar_sel50":  self.scalar_sel50,
            "scalar_sel100": self.scalar_sel100,
            "scalar_sel1k":  self.scalar_sel1k,
            "scalar_sel2k":  self.scalar_sel2k,
            "scalar_sel3k":  self.scalar_sel3k,
            "scalar_sel4k":  self.scalar_sel4k,
            "scalar_sel5k":  self.scalar_sel5k,
            "arr_unique": self.arr_unique,
            "arr_sel5":   self.arr_sel5,
            "arr_sel10":  self.arr_sel10,
            "arr_sel50":  self.arr_sel50,
            "arr_sel100": self.arr_sel100,
            "arr_sel1k":  self.arr_sel1k,
            "arr_sel2k":  self.arr_sel2k,
            "arr_sel3k":  self.arr_sel3k,
            "arr_sel4k":  self.arr_sel4k,
            "arr_sel5k":  self.arr_sel5k,
            "pad": self._padding,
        }

    @post_load
    def finish_schema_info(self):
        """Print schema configuration after data load. Inherited by all
        subclasses so the schema summary shows in every run."""
        n = self.document_count or 100000
        print(f"\n{'='*60}")
        print(f"Find/Read Schema loaded: {n:,} documents")
        print(f"  10 scalar fields (unique, sel5..sel5k)")
        print(f"  10 array fields  (unique, sel5..sel5k, array_size=3)")
        print(f"  1 padding field")
        print(f"{'='*60}\n")
