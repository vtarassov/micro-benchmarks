"""
Find/Read Data Loader — concrete loader that drops the collection, inserts
N documents using the FindReadWorkload schema, and quits cleanly.

Usage:
  bash test/run_locust.sh \\
    --locustfile workloads/read_queries/find_read_loader.py \\
    --users 70 --run-time 9999s \\
    --document-count 1000000 --load-batch-size 100

The loader auto-quits once the data load completes (via ``@post_load``
sending a quit message to master). Smart-skip also works: if the collection
already has >= ``--document-count`` docs, the load is skipped.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, os.path.dirname(__file__))

from find_read_base import FindReadWorkload, _QUIT_MESSAGE_NAME
from perf_test_user import post_load, workload


class FindReadLoader(FindReadWorkload):
    """Concrete loader. Inherits schema and lifecycle from FindReadWorkload,
    then adds:
    - ``@post_load send_quit``: notifies master to stop all workers
    - ``@workload noop``: Locust requires at least one @workload method
    """

    abstract = False

    @post_load
    def send_quit(self):
        """Tell master to quit all workers after data load + schema info print."""
        print("[POST_LOAD] Loader done. Sending quit message to master...")
        self.environment.runner.send_message(_QUIT_MESSAGE_NAME, "loader_done")

    @workload(weight=1, name="noop")
    def noop(self):
        """No-op fallback. Not reached in practice — send_quit above stops the
        test before WORKLOAD phase kicks off."""
        import time
        time.sleep(0.1)
