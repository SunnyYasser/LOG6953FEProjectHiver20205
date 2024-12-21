#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

run_with_profiling() {
    test_command=$1

    if [ -z "$test_command" ]; then
        echo "Error: No test command provided!"
        echo "Usage: ./run_with_profiling.sh <executable>"
        exit 1
    fi

    echo "Running test: $test_command with profiling..."
    echo "==============================================="

    echo "Performance Metrics (CPU and Memory):"
    perf stat -e \
        instructions,cycles,branches,branch-misses,cache-references,\
cache-misses,page-faults,dTLB-load-misses,dTLB-loads "$test_command"

    echo ""
    echo "Memory Usage:"
    memory_info=$(/usr/bin/time -v "$test_command" 2>&1 | grep 'Maximum resident set size')

    # Extract peak memory and convert to MB
    peak_memory_kb=$(echo "$memory_info" | awk '{print $6}')
    peak_memory_mb=$(echo "scale=2; $peak_memory_kb / 1024" | bc)

    echo "  Peak Memory Usage: ${peak_memory_mb} MB"
    echo "==============================================="
    echo "Test $test_command completed successfully!"
}

run_with_profiling "$1"