import re
from dataclasses import dataclass
from typing import List, Dict, Union
import statistics
import sys
import csv
import json
from datetime import datetime
import subprocess

@dataclass
class TestMetrics:
    test_name: str
    query: str
    column_ordering: str
    operators: Dict[str, List[int]]
    instructions_atom: List[int]
    cycles_atom: List[int]
    page_faults: List[int]
    branch_misses_atom: List[int]
    cache_misses_atom: List[int]
    elapsed_time: List[float]
    peak_memory: List[float]
    is_packed: int
    speedup: float = 0.0
    equivalent_query: str = "N/A"


def get_git_commit_hash():
    try:
        return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()
    except:
        return "unknown"

def parse_number(num_str: str) -> int:
    return int(num_str.replace(',', ''))


def get_sink_type(operators: Union[Dict[str, List[int]], Dict[str, int]]) -> str:
    """
    Determine the type of sink operator being used.
    Returns: 'no_op', 'packed', 'regular', or None
    """
    if isinstance(next(iter(operators.values())), list):
        # Handle List[int] case
        if 'SINK_NO_OP' in operators:
            return 'no_op'
        elif 'SINK_PACKED' in operators:
            return 'packed'
        elif 'SINK' in operators:
            return 'regular'
    else:
        # Handle int case
        if 'SINK_NO_OP' in operators and operators['SINK_NO_OP'] > 0:
            return 'no_op'
        elif 'SINK_PACKED' in operators and operators['SINK_PACKED'] > 0:
            return 'packed'
        elif 'SINK' in operators and operators['SINK'] > 0:
            return 'regular'
    return None

def are_execution_patterns_compatible(packed_test: TestMetrics, base_test: TestMetrics) -> bool:
    """
    Check if a packed test and base test have compatible execution patterns.
    """
    sink_type_packed = get_sink_type(packed_test.operators)
    sink_type_base = get_sink_type(base_test.operators)

    # If either test has SINK_NO_OP, they must match exactly
    if 'no_op' in (sink_type_packed, sink_type_base):
        return sink_type_packed == sink_type_base

    # For packed vs unpacked, ensure one is SINK_PACKED and other is regular SINK
    return (sink_type_packed == 'packed' and sink_type_base == 'regular')

def are_queries_equivalent(query1: TestMetrics, query2: TestMetrics) -> bool:
    """Check if two queries are equivalent (same query and ordering)"""
    return (query1.query == query2.query and
            query1.column_ordering == query2.column_ordering)

def calculate_speedups(packed_tests: List[TestMetrics], base_tests: List[TestMetrics]):
    """
    Calculate speedup for packed queries compared to their unpacked counterparts.
    Matches queries based on compatible execution patterns.
    """
    for packed_test in packed_tests:
        matching_base_tests = [
            test for test in base_tests
            if (are_queries_equivalent(test, packed_test) and    # Same query and ordering
                are_execution_patterns_compatible(packed_test, test))  # Compatible sink types
        ]

        if matching_base_tests:
            base_test = matching_base_tests[0]
            base_time = statistics.median(base_test.elapsed_time)
            packed_time = statistics.median(packed_test.elapsed_time)

            if packed_time > 0:
                packed_test.speedup = base_time / packed_time
                packed_test.equivalent_query = f"Test {base_test.test_name}"



def parse_log_file(content: str, num_runs: int) -> List[TestMetrics]:
    tests = []
    test_sections = re.split(r'Running test:', content)[1:]

    current_test = None
    run_count = 0

    for section in test_sections:
        test_path_match = re.search(r'(.+?) with profiling', section)
        if not test_path_match:
            continue

        test_path = test_path_match.group(1).strip()
        test_name = test_path.split('/')[-1]

        query_match = re.search(r'Test \d+: ([^\n]+)', section)
        query = query_match.group(1) if query_match else "N/A"

        column_ordering_match = re.search(r'COLUMN ORDERING: ([^\n]+)', section)
        column_ordering = column_ordering_match.group(1) if column_ordering_match else "N/A"

        test_num_match = re.search(r'test(\d+)', test_name)
        test_num = test_num_match.group(1) if test_num_match else "N/A"

        if current_test is None or (current_test.test_name != test_num or
                                    current_test.query != query or
                                    current_test.column_ordering != column_ordering):

            if current_test is not None:
                tests.append(current_test)

            current_test = TestMetrics(
                test_name=test_num,
                query=query,
                column_ordering=column_ordering,
                operators={},
                instructions_atom=[],
                cycles_atom=[],
                page_faults=[],
                elapsed_time=[],
                peak_memory=[],
                branch_misses_atom=[],
                cache_misses_atom=[],
                is_packed=0
            )
            run_count = 0

        operator_matches = re.findall(r'((?:SCAN|INLJ(?:_PACKED)?\d*|SINK(?:_PACKED|_NO_OP)?)).*?: (\d+)', section)
        for op_name, count in operator_matches:
            if op_name not in current_test.operators:
                current_test.operators[op_name] = []
            current_test.operators[op_name].append(int(count))

        current_test.is_packed = 1 if any(('PACKED' in op) for op in current_test.operators.keys()) else 0

        instr_match = re.search(r'([\d,]+)\s+.*?instructions', section)
        if instr_match:
            current_test.instructions_atom.append(parse_number(instr_match.group(1)))

        cycles_match = re.search(r'([\d,]+)\s+.*?cycles', section)
        if cycles_match:
            current_test.cycles_atom.append(parse_number(cycles_match.group(1)))

        branch_misses_match = re.search(r'([\d,]+)\s+.*?branch-misses', section)
        if branch_misses_match:
            current_test.branch_misses_atom.append(parse_number(branch_misses_match.group(1)))

        cache_misses_match = re.search(r'([\d,]+)\s+.*?cache-misses', section)
        if cache_misses_match:
            current_test.cache_misses_atom.append(parse_number(cache_misses_match.group(1)))

        faults_match = re.search(r'([\d,]+)\s+.*?page-faults', section)
        if faults_match:
            current_test.page_faults.append(parse_number(faults_match.group(1)))

        time_match = re.findall(r'Execution time: (\d+) ms', section)
        if time_match:
            for time in time_match:
                current_test.elapsed_time.append(float(time))

        memory_match = re.findall(r'Peak Memory Usage: (\d+\.\d+)', section)
        if memory_match:
            for mem in memory_match:
                current_test.peak_memory.append(float(mem))

        run_count += 1
        if run_count == num_runs and current_test is not None:
            tests.append(current_test)
            current_test = None

    if current_test is not None:
        tests.append(current_test)

    return tests

def write_experimental_results(tests: List[TestMetrics], experiment_name: str, output_path: str):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    commit_hash = get_git_commit_hash()
    experiment_id = f"{timestamp}_{commit_hash[:8]}"

    # Split tests into packed and base
    packed_tests = [test for test in tests if test.is_packed == 1]
    base_tests = [test for test in tests if test.is_packed == 0]

    # Calculate speedups before writing results
    calculate_speedups(packed_tests, base_tests)

    with open(f"{output_path}/experiments.csv", 'a', newline='') as f:
        writer = csv.writer(f)
        if f.tell() == 0:  # File is empty, write header
            writer.writerow(['experiment_id', 'experiment_name', 'timestamp', 'commit_hash', 'config'])
        writer.writerow([experiment_id, experiment_name, timestamp, commit_hash, json.dumps(None)])

    with open(f"{output_path}/queries.csv", 'a', newline='') as f:
        writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if f.tell() == 0:  # File is empty, write header
            writer.writerow(['experiment_id', 'query_id', 'ordering', 'is_packed', 'metrics', 'speedup', 'equivalent_query'])

        for test in tests:
            metrics = {
                'operators': [test.operators[op][0] for op in test.operators],
                'instructions': test.instructions_atom,
                'cycles': test.cycles_atom,
                'page_faults': test.page_faults,
                'branch_misses': test.branch_misses_atom,
                'cache_misses': test.cache_misses_atom,
                'time_ms': test.elapsed_time,
                'memory_mb': test.peak_memory
            }

            writer.writerow([
                experiment_id,
                test.test_name,
                test.column_ordering,
                test.is_packed,
                json.dumps(metrics, ensure_ascii=False),
                test.speedup,
                test.equivalent_query
            ])


def main():
    if len(sys.argv) != 5:
        print("Usage: python script.py <log_file_path> <num_runs> <experiment_name> <output_path>")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        content = f.read()

    num_runs = int(sys.argv[2])
    experiment_name = sys.argv[3]
    output_path = sys.argv[4]
    tests = parse_log_file(content, num_runs)
    write_experimental_results(tests, experiment_name, output_path)
    print(f"Results written to experiments.csv and queries.csv")

if __name__ == "__main__":
    main()
