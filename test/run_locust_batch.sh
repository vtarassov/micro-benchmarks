#!/bin/bash
#
# Batch Locust launcher script
# Inspects all input parameters and expands comma-separated lists
# Invokes run_locust.sh for each combination
#

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Arrays to store parameter names and their values (possibly comma-separated)
declare -A PARAMS
declare -a PARAM_ORDER

# Special parameter for output directory
OUTPUT_DIR=""

# Track which parameters have multiple values (comma-separated lists)
declare -A MULTI_VALUE_PARAMS

# Parse command line options - accept any --parameter format
while [[ $# -gt 0 ]]; do
    if [[ $1 == --* ]]; then
        PARAM_NAME="${1#--}"  # Remove leading --

        # Check if there's a value following this parameter
        if [[ -n "$2" && "$2" != --* ]]; then
            # Handle output-dir specially
            if [[ "$PARAM_NAME" == "output-dir" ]]; then
                OUTPUT_DIR="$2"
                shift 2
            else
                PARAMS["$PARAM_NAME"]="$2"
                PARAM_ORDER+=("$PARAM_NAME")
                shift 2
            fi
        else
            echo "Error: Parameter $1 requires a value"
            exit 1
        fi
    else
        echo "Unknown option: $1"
        echo "All parameters must start with --"
        exit 1
    fi
done

# Function to recursively generate all combinations
generate_combinations() {
    local depth=$1
    shift
    local -a current_values=("$@")

    if [[ $depth -eq ${#PARAM_ORDER[@]} ]]; then
        # We've set all parameters, now run the command
        local cmd="$SCRIPT_DIR/run_locust.sh"

        # Build the CSV filename if output-dir is specified
        local csv_path=""
        if [[ -n "$OUTPUT_DIR" ]]; then
            # Build filename: results_ + param1_value1_param2_value2...
            # Only include parameters that have multiple values (comma-separated lists)
            local csv_filename="results"
            for i in "${!PARAM_ORDER[@]}"; do
                local param_name="${PARAM_ORDER[$i]}"
                # Only include this parameter if it has multiple values
                if [[ -n "${MULTI_VALUE_PARAMS[$param_name]}" ]]; then
                    local param_value="${current_values[$i]}"
                    # Replace slashes and spaces with underscores for safe filenames
                    param_value=$(echo "$param_value" | tr '/ ' '__')
                    csv_filename="${csv_filename}_${param_name}_${param_value}"
                fi
            done
            csv_path="${OUTPUT_DIR}/${csv_filename}"
        fi

        echo ""
        echo "=========================================="
        echo -n "Running: $cmd"
        for i in "${!PARAM_ORDER[@]}"; do
            echo -n " --${PARAM_ORDER[$i]} \"${current_values[$i]}\""
        done
        if [[ -n "$csv_path" ]]; then
            echo -n " --csv \"$csv_path\""
        fi
        echo ""
        echo "=========================================="

        # Build the actual command with proper quoting
        local run_cmd=("$cmd")
        for i in "${!PARAM_ORDER[@]}"; do
            run_cmd+=("--${PARAM_ORDER[$i]}" "${current_values[$i]}")
        done

        # Add CSV parameter if output-dir was specified
        if [[ -n "$csv_path" ]]; then
            run_cmd+=("--csv" "$csv_path")
        fi

        "${run_cmd[@]}"

        return
    fi

    local current_param="${PARAM_ORDER[$depth]}"
    local values_str="${PARAMS[$current_param]}"

    # Split the values by comma
    IFS=',' read -ra values <<< "$values_str"

    for value in "${values[@]}"; do
        # Trim whitespace from value
        value=$(echo "$value" | xargs)
        generate_combinations $((depth + 1)) "${current_values[@]}" "$value"
    done
}

# Check if any parameters were provided
if [[ ${#PARAMS[@]} -eq 0 ]]; then
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "This script accepts any parameters that run_locust.sh accepts."
    echo "Any parameter value can be a comma-separated list to run multiple combinations."
    echo ""
    echo "Common options:"
    echo "  --output-dir DIR        Output directory for CSV results (optional)"
    echo "  --locustfile FILE       Path to locustfile"
    echo "  --uri URI               MongoDB connection URI"
    echo "  --users NUM             Number of users"
    echo "  --spawn-rate NUM        Spawn rate per second"
    echo "  --run-time TIME         Run time (e.g., 100s, 1m)"
    echo "  --document-count NUM    Number of documents"
    echo "  --load-batch-size NUM   Load batch size"
    echo ""
    echo "Examples:"
    echo "  $0 --output-dir ./results --users 10,20,30 --document-count 1000,5000"
    echo "  $0 --uri mongodb://localhost:27017 --users 100"
    echo "  $0 --output-dir ./output --users 50,100 --spawn-rate 10,20 --run-time 1m,5m"
    exit 1
fi

echo "=========================================="
echo "Batch Locust Runner"
echo "=========================================="
echo ""
if [[ -n "$OUTPUT_DIR" ]]; then
    echo "Output directory: $OUTPUT_DIR"
    echo ""
fi
echo "Parameters:"
for param in "${PARAM_ORDER[@]}"; do
    IFS=',' read -ra values <<< "${PARAMS[$param]}"
    if [[ ${#values[@]} -gt 1 ]]; then
        echo "  $param: ${PARAMS[$param]} (${#values[@]} values)"
        MULTI_VALUE_PARAMS["$param"]=1
    else
        echo "  $param: ${PARAMS[$param]}"
    fi
done
echo ""

# Calculate total number of runs
total_runs=1
for param in "${PARAM_ORDER[@]}"; do
    IFS=',' read -ra values <<< "${PARAMS[$param]}"
    total_runs=$((total_runs * ${#values[@]}))
done

echo "Total runs: $total_runs"
echo ""

# Create output directory if specified
if [[ -n "$OUTPUT_DIR" ]]; then
    if [[ ! -d "$OUTPUT_DIR" ]]; then
        echo "Creating output directory: $OUTPUT_DIR"
        mkdir -p "$OUTPUT_DIR"
        echo ""
    fi
fi

# Generate and execute all combinations
generate_combinations 0

echo ""
echo "=========================================="
echo "All batch runs completed!"
echo "=========================================="
