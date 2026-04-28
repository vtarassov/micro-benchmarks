"""
Range Query on Array Field — parameterized by --selectivity.

Single @workload: bounded 30% range filter on arr_sel{N}, exhaust (no limit).
Index: created in @post_load on the target field only (multikey index).

Usage:
  locust -f workloads/range_arr.py --uri=... --document-count=1000000 --selectivity=100
"""

import sys
import os
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, os.path.dirname(__file__))

from find_read_base import FindReadWorkload, SELECTIVITY_CHOICES, parse_selectivity
from perf_test_user import workload, pre_load, post_load, get_parsed_args
from locust import events


@events.init_command_line_parser.add_listener
def _setup_parser(parser):
    parser.add_argument('--selectivity', type=str, default='100',
                        choices=SELECTIVITY_CHOICES,
                        help='Selectivity level (default: 100)')


class RangeArr(FindReadWorkload):
    """Bounded 30% range query on array field at specified selectivity. No limit (exhaust)."""

    abstract = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        args = get_parsed_args()
        sel_str = args.selectivity if args and hasattr(args, 'selectivity') else '100'
        suffix, self._sel_value = parse_selectivity(sel_str)
        self._field_name = f"arr_{suffix}"
        self._field = getattr(self, self._field_name)

        if suffix == "unique":
            self._query_name = "range_arr_unique_30pct"
        else:
            self._query_name = f"range_arr_{suffix}_30pct"

    @post_load
    def setup_index(self):
        """Drop all indexes, create single multikey index on target field.
        Skipped when --skip-index-setup is set."""
        args = get_parsed_args()
        if args and getattr(args, 'skip_index_setup', False):
            print(f"[POST_LOAD] --skip-index-setup set, keeping existing indexes")
            return
        print(f"Dropping all indexes...")
        self.collection.drop_indexes()
        print(f"Creating index on {self._field_name}...")
        self.collection.create_index([(self._field_name, 1)])
        print(f"Index {self._field_name}_1 ready")

    @workload(weight=1, name="query")
    def run_query(self):
        start_pct = random.uniform(0, 70)
        low, _ = self._field.get_percentile(start_pct)
        high, _ = self._field.get_percentile(start_pct + 30.0)
        self.db.command({
            "find": self.collection_name,
            "filter": {self._field_name: {"$gte": low, "$lte": high}},
            "batchSize": 10000000,
            "singleBatch": True,
            "maxTimeMS": 600000,
        })

    def on_start(self):
        super().on_start()
        for i, (method, weight, name) in enumerate(self._workloads):
            if name == "query":
                self._workloads[i] = (method, weight, self._query_name)
                break
