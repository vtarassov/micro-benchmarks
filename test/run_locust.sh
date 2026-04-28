#!/bin/bash
#
# Multi-process Locust launcher script
# Starts one master and one worker per CPU core
# Properly kills all child processes on exit
#

set -e

# Array to collect extra parameters to pass through to locust
EXTRA_PARAMS=()

# Parse command line options
while [[ $# -gt 0 ]]; do
    case $1 in
        --locustfile) LOCUSTFILE="$2"; shift 2 ;;
        --uri) URI="$2"; shift 2 ;;
        --users) USERS="$2"; shift 2 ;;
        --spawn-rate) SPAWN_RATE="$2"; shift 2 ;;
        --run-time) RUN_TIME="$2"; shift 2 ;;
        --document-count) DOCUMENT_COUNT="$2"; shift 2 ;;
        --load-batch-size) LOAD_BATCH_SIZE="$2"; shift 2 ;;
        --*)
            # Unknown parameter - collect it to pass through to locust
            EXTRA_PARAMS+=("$1")
            if [[ -n "$2" && "$2" != --* ]]; then
                EXTRA_PARAMS+=("$2")
                shift 2
            else
                shift 1
            fi
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCUSTFILE="${LOCUSTFILE:-$SCRIPT_DIR/example.py}"
URI="${URI:-${LOCUST_HOST:-mongodb://localhost:27017/?retryWrites=false}}"
USERS="${USERS:-${LOCUST_USERS:-100}}"
SPAWN_RATE="${SPAWN_RATE:-${LOCUST_SPAWN_RATE:-50}}"
RUN_TIME="${RUN_TIME:-${LOCUST_RUN_TIME:-120s}}"
DOCUMENT_COUNT="${DOCUMENT_COUNT:-1000000}"
LOAD_BATCH_SIZE="${LOAD_BATCH_SIZE:-1}"

# Get number of CPU cores, cap workers at --users count (no point having idle workers)
NUM_CORES=$(nproc)
if [ "$USERS" -lt "$NUM_CORES" ]; then
    NUM_WORKERS=$USERS
else
    NUM_WORKERS=$NUM_CORES
fi
echo "Detected $NUM_CORES CPU cores, using $NUM_WORKERS workers (--users=$USERS)"

# Array to track all PIDs
declare -a PIDS=()

# Cleanup function to kill all spawned processes
cleanup() {
    echo ""
    echo "Shutting down all Locust processes..."

    # Kill all tracked PIDs
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing process $pid"
            kill "$pid" 2>/dev/null || true
        fi
    done

    # Wait a moment for graceful shutdown
    sleep 2

    # Force kill any remaining processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force killing process $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done

    echo "All processes terminated"
    exit 0
}

# Set up trap to catch SIGINT (Ctrl+C) and SIGTERM
trap cleanup SIGINT SIGTERM EXIT

# Standalone mode for single worker, distributed mode otherwise
if [ "$NUM_WORKERS" -le 1 ]; then
    echo "Starting Locust in standalone mode (single process)..."
    locust -f "$LOCUSTFILE" --uri="$URI" --headless --users="$USERS" \
        --spawn-rate="$SPAWN_RATE" --run-time="$RUN_TIME" \
        --document-count="$DOCUMENT_COUNT" --load-batch-size="$LOAD_BATCH_SIZE" \
        "${EXTRA_PARAMS[@]}" &
    LOCUST_PID=$!
    PIDS+=($LOCUST_PID)

    echo ""
    echo "=========================================="
    echo "Locust is running in standalone mode"
    echo "Users: $USERS"
    echo "Spawn rate: $SPAWN_RATE per second"
    echo "Run time: $RUN_TIME"
    echo "Document count: $DOCUMENT_COUNT"
    echo "Load batch size: $LOAD_BATCH_SIZE"
    if [[ ${#EXTRA_PARAMS[@]} -gt 0 ]]; then
        echo "Extra parameters: ${EXTRA_PARAMS[*]}"
    fi
    echo "Press Ctrl+C to stop"
    echo "=========================================="
    echo ""
else
    # Start master process
    echo "Starting Locust master process..."
    locust -f "$LOCUSTFILE" --uri="$URI" --master --headless --users="$USERS" \
        --spawn-rate="$SPAWN_RATE" --run-time="$RUN_TIME" --expect-workers="$NUM_WORKERS" \
        --document-count="$DOCUMENT_COUNT" --load-batch-size="$LOAD_BATCH_SIZE" \
        "${EXTRA_PARAMS[@]}" &
    MASTER_PID=$!
    PIDS+=($MASTER_PID)
    echo "Master process started (PID: $MASTER_PID)"

    # Give master a moment to start
    sleep 2

    # Start worker processes
    echo "Starting $NUM_WORKERS worker processes..."
    for i in $(seq 1 $NUM_WORKERS); do
        locust -f "$LOCUSTFILE" --uri="$URI" --worker "${EXTRA_PARAMS[@]}" &
        WORKER_PID=$!
        PIDS+=($WORKER_PID)
        echo "Worker $i started (PID: $WORKER_PID)"
    done

    echo ""
    echo "=========================================="
    echo "Locust is running with $NUM_WORKERS workers"
    echo "Users: $USERS"
    echo "Spawn rate: $SPAWN_RATE per second"
    echo "Run time: $RUN_TIME"
    echo "Document count: $DOCUMENT_COUNT"
    echo "Load batch size: $LOAD_BATCH_SIZE"
    if [[ ${#EXTRA_PARAMS[@]} -gt 0 ]]; then
        echo "Extra parameters: ${EXTRA_PARAMS[*]}"
    fi
    echo "Press Ctrl+C to stop all processes"
    echo "=========================================="
    echo ""
fi

# Wait for all processes
wait
