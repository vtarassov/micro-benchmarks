"""PerfTestContext class for managing shared state across performance test processes."""

from typing import Optional
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import resource_tracker
from enum import Enum
import numpy as np
import posix_ipc
import time
import warnings


class TestPhase(Enum):
    PRE_LOAD = 0
    DATA_LOAD = 1
    POST_LOAD = 2
    WORKLOAD = 3


class DataGenerationContext:
    """
    Lightweight context object passed to document shape functions.

    Contains read-only attributes for document generation, populated by
    PerfTestContext when creating each document.
    """

    def __init__(self):
        """Initialize an empty data generation context."""
        self.document_number: int = 0
        self.locust_user_id: int = 0
        self.shape_ordinal: int = 0  # Ordinal within the current document shape
        self.shape_max_count: Optional[int] = None  # Maximum documents for this shape


class PerfTestContext:
    """
    Context object for synchronizing data across processes and test phases.

    This class provides shared state management for performance tests, including
    atomic counters that can be safely accessed from multiple worker processes.

    Uses numpy structured array for shared memory:
    - document_counter: uint64 - atomic counter for document generation
    - current_phase: uint8 - current test phase (0=PRE_LOAD, 1=DATA_LOAD, 2=POST_LOAD, 3=WORKLOAD)
    - locust_user_counter: uint64 - atomic counter for locust user registration
    - active_data_loaders: uint64 - number of users currently loading data

    Uses posix_ipc.Semaphore for process-safe locking.
    """

    # Define numpy dtype for structured array
    SHARED_DTYPE = np.dtype([
        ('document_counter', np.uint64),
        ('current_phase', np.uint8),
        ('locust_user_counter', np.uint64),
        ('active_data_loaders', np.uint64),
    ])

    def __init__(self, is_master: bool = False):
        """Initialize the context with shared atomic counters.

        Args:
            is_master: Whether to create new shared memory (True) or attach to existing (False)
        """
        # Create or attach to shared memory
        self._shm_name = "perf_test_context"
        self._sem_name = "perf_test_sem"
        self._is_master = is_master  # Track if we created the resources

        if self._is_master:
            # Clean up any existing resources
            try:
                existing_sem = posix_ipc.Semaphore(self._sem_name)
                existing_sem.close()
                existing_sem.unlink()
            except posix_ipc.ExistentialError:
                pass

            try:
                existing_shm = SharedMemory(name=self._shm_name)
                existing_shm.close()
                existing_shm.unlink()
            except FileNotFoundError:
                pass

            # Create new shared memory and semaphore
            try:
                self._shm = SharedMemory(name=self._shm_name, create=True, size=self.SHARED_DTYPE.itemsize)
                # Create numpy array view on shared memory
                self._data = np.ndarray(shape=(1,), dtype=self.SHARED_DTYPE, buffer=self._shm.buf)
                # Initialize to zeros
                self._data[0]['document_counter'] = 0
                self._data[0]['current_phase'] = TestPhase.PRE_LOAD.value
                self._data[0]['locust_user_counter'] = 0
                self._data[0]['active_data_loaders'] = 0
            except FileExistsError as e:
                # Already exists, just attach to it
                self._shm = SharedMemory(name=self._shm_name)
                self._data = np.ndarray(shape=(1,), dtype=self.SHARED_DTYPE, buffer=self._shm.buf)

            try:
                self._sem = posix_ipc.Semaphore(self._sem_name, flags=posix_ipc.O_CREX, initial_value=1)
            except posix_ipc.ExistentialError:
                # Already exists, just open it
                self._sem = posix_ipc.Semaphore(self._sem_name)
        else:
            # Attach to existing resources
            self._shm = SharedMemory(name=self._shm_name)
            self._data = np.ndarray(shape=(1,), dtype=self.SHARED_DTYPE, buffer=self._shm.buf)
            self._sem = posix_ipc.Semaphore(self._sem_name)

        # Unregister from resource tracker - we manage cleanup manually
        # This prevents "leaked shared_memory" warnings at shutdown
        try:
            resource_tracker.unregister(self._shm._name, "shared_memory")
        except Exception:
            pass

    def next_document_numbers(self, count: int = 1) -> tuple[int, int]:
        """
        Get a range of document numbers in a single atomic operation.

        This is more efficient than calling this method multiple times
        when you need to retrieve multiple document numbers for batch operations.

        Args:
            count: The number of document numbers to retrieve (default: 1)

        Returns:
            tuple[int, int]: A tuple of (start_number, end_number) where end_number
                            is exclusive (i.e., the range is [start_number, end_number))
        """
        self._sem.acquire()
        try:
            start = int(self._data[0]['document_counter'])
            self._data[0]['document_counter'] = start + count
            return (start, start + count)
        finally:
            self._sem.release()

    def next_document_number(self) -> int:
        """
        Get the next document number in a thread-safe manner.

        Returns:
            int: A unique, incrementing document number
        """
        start, _ = self.next_document_numbers(1)
        return start

    def get_document_count(self) -> int:
        """
        Get the current document count without incrementing.

        Returns:
            int: The current document count
        """
        self._sem.acquire()
        try:
            return int(self._data[0]['document_counter'])
        finally:
            self._sem.release()

    def create_data_generation_context(self) -> DataGenerationContext:
        """
        Create a new DataGenerationContext with current state.

        This method atomically increments the document number and creates
        a lightweight context object to pass to document shape functions.

        Returns:
            DataGenerationContext: A new context with populated attributes
        """
        ctx = DataGenerationContext()
        ctx.document_number = self.next_document_number()
        return ctx

    def get_current_phase(self) -> TestPhase:
        """
        Get the current test phase.

        Returns:
            TestPhase: The current phase of the test
        """
        self._sem.acquire()
        try:
            phase_value = int(self._data[0]['current_phase'])
            return TestPhase(phase_value)
        finally:
            self._sem.release()

    def set_current_phase(self, phase: TestPhase) -> None:
        """
        Set the current test phase.

        Args:
            phase: The new test phase
        """
        self._sem.acquire()
        try:
            self._data[0]['current_phase'] = phase.value
        finally:
            self._sem.release()

    def register_locust_user(self) -> int:
        """
        Register a locust user and get a unique locust user ID.

        Also increments the active data loaders counter.

        Returns:
            int: A unique locust user ID (starting from 0)
        """
        self._sem.acquire()
        try:
            locust_user_id = int(self._data[0]['locust_user_counter'])
            self._data[0]['locust_user_counter'] = locust_user_id + 1
            # Increment active data loaders count
            self._data[0]['active_data_loaders'] += 1
            return locust_user_id
        finally:
            self._sem.release()

    def decrement_active_data_loaders(self) -> None:
        """
        Decrement the active data loaders counter.

        If the counter reaches zero, automatically transitions to POST_LOAD phase.
        This should be called when a user completes data loading.
        """
        self._sem.acquire()
        try:
            if self._data[0]['active_data_loaders'] > 0:
                self._data[0]['active_data_loaders'] -= 1

                # If all users are done loading, transition to POST_LOAD
                if self._data[0]['active_data_loaders'] == 0:
                    current_phase = TestPhase(int(self._data[0]['current_phase']))
                    if current_phase == TestPhase.DATA_LOAD:
                        self._data[0]['current_phase'] = TestPhase.POST_LOAD.value
        finally:
            self._sem.release()

    def cleanup(self) -> None:
        """
        Cleanup shared memory and semaphore resources for workers.

        Workers (non-master) should call this to close their handles without unlinking.
        Should be called when a worker is done using the context.
        """
        if not self._is_master:
            # Workers just close their handles
            try:
                self._sem.close()
            except Exception:
                pass

            try:
                self._shm.close()
            except Exception:
                pass

    def teardown(self) -> None:
        """
        Teardown shared memory and semaphore resources for master.

        Master should call this to close AND unlink resources during shutdown.
        This ensures proper cleanup and prevents resource leaks.
        """
        if self._is_master:
            # Master must close before unlinking to avoid warnings
            try:
                self._sem.close()
            except Exception:
                pass

            try:
                self._sem.unlink()
            except Exception:
                pass

            try:
                self._shm.close()  # IMPORTANT: Close before unlink
            except Exception:
                pass

            try:
                self._shm.unlink()
            except Exception:
                pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ARG002
        """Context manager exit - ensures proper cleanup."""
        if self._is_master:
            self.teardown()
        else:
            self.cleanup()
        return False

    def __del__(self):
        """
        Destructor to ensure cleanup happens even if not explicitly called.

        This is a safety net to prevent resource leaks if cleanup/teardown
        are not explicitly invoked.
        """
        try:
            if self._is_master:
                self.teardown()
            else:
                self.cleanup()
        except Exception:
            # Ignore all errors in destructor
            pass


__all__ = ['PerfTestContext', 'DataGenerationContext', 'TestPhase']
