#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

run_with_profiling() {
   test_command=$1
   num_runs=$2

   if [ -z "$test_command" ]; then
       echo "Error: No test command provided!"
       echo "Usage: ./run_with_profiling.sh <executable> <num_runs>"
       exit 1
   fi

   if [ -z "$num_runs" ]; then
       echo "Error: Number of runs not specified!"
       echo "Usage: ./run_with_profiling.sh <executable> <num_runs>"
       exit 1
   fi

   echo "Running test: $test_command with profiling..."
   echo "==============================================="

   echo "Performance Metrics (CPU and Memory):"
   perf stat -r "$num_runs" -e \
       instructions,cycles,branches,branch-misses,cache-references,\
cache-misses,page-faults,dTLB-load-misses,dTLB-loads "$test_command"

   echo ""
   echo "==============================================="
   echo "Test $test_command completed successfully!"
}

run_with_profiling "$1" "$2"