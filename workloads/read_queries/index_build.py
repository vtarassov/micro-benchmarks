"""
Index Build Test — measure index creation time on the find_read schema.

Drops all indexes, builds one index on specified fields, measures time.
Data loading is handled automatically by the framework's smart-skip detection.

Usage:
  # Single column index
  locust -f workloads/read_queries/index_build.py \
    --uri="mongodb://..." --document-count=1000000 \
    --index-fields=scalar_sel100 \
    --headless --users=1 --spawn-rate=1 --run-time=9999s

  # Compound index (arr prefix, scalar suffix)
  locust -f workloads/read_queries/index_build.py \
    --uri="mongodb://..." --document-count=1000000 \
    --index-fields=arr_sel100,scalar_sel100 \
    --headless --users=1 --spawn-rate=1 --run-time=9999s
"""

import sys
import os
import time as _time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, os.path.dirname(__file__))

from find_read_base import FindReadWorkload, _QUIT_MESSAGE_NAME
from perf_test_user import pre_load, post_load, workload, get_parsed_args
from locust import events


@events.init_command_line_parser.add_listener
def _setup_parser(parser):
    parser.add_argument('--index-fields', type=str, required=True,
                        help='Comma-separated field names for index columns, in order. '
                             'e.g. "scalar_sel100" or "arr_sel100,scalar_sel100"')
    parser.add_argument('--background', action='store_true', default=False,
                        help='Create index in background mode')


class IndexBuild(FindReadWorkload):
    """Measures index build time on the find_read schema."""

    abstract = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        args = get_parsed_args()
        fields_str = args.index_fields if args and hasattr(args, 'index_fields') else 'scalar_sel100'
        self._index_fields = [f.strip() for f in fields_str.split(',')]
        self._index_spec = [(f, 1) for f in self._index_fields]
        self._background = args.background if args and hasattr(args, 'background') else False
        self._index_name = '_'.join(self._index_fields)

    @post_load
    def build_index(self):
        """Drop all indexes, then build the specified index and measure time."""
        print(f"\n{'='*60}")
        print(f"[INDEX_BUILD] Dropping all indexes...")
        self.collection.drop_indexes()

        print(f"[INDEX_BUILD] Building index: {self._index_spec}")
        print(f"[INDEX_BUILD] Background: {self._background}")
        start = _time.time()
        self.collection.create_index(self._index_spec, background=self._background)
        elapsed = _time.time() - start
        print(f"[INDEX_BUILD] Index build completed in {elapsed:.2f} seconds")
        print(f"{'='*60}\n")

        # Quit cleanly via master (see find_read_base._QUIT_MESSAGE_NAME)
        print("[INDEX_BUILD] Sending quit message to master...")
        self.environment.runner.send_message(_QUIT_MESSAGE_NAME, f"index_build_done: {self._index_name}")

    @workload(weight=1, name="noop")
    def noop(self):
        """Fallback — should not be reached if runner.quit() works."""
        import time
        time.sleep(0.1)
