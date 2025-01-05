import re
from dataclasses import dataclass
from typing import List, Dict, Optional
import sys
from prettytable import PrettyTable
import statistics
import csv
from typing import Union
import json

@dataclass
class TestMetrics:
    test_name: str
    query: str
    column_ordering: str
    operators: Dict[str, List[int]]
    instructions_atom: List[int]
    cycles_atom: List[int]
    page_faults: List[int]
    elapsed_time: List[float]
    peak_memory: List[float]
    branch_misses_atom: List[int]
    cache_misses_atom: List[int]
    is_packed: int
    speedup: float = 0.0
    equivalent_query: str = "N/A"


def has_packed_inlj(operators: Union[Dict[str, List[int]], Dict[str, int]]) -> bool:
    """Check if any INLJ_PACKED operator has non-zero count"""
    for op, counts in operators.items():
        if op.startswith('INLJ_PACKED'):
            # Handle both List[int] and int cases
            if isinstance(counts, list):
                if any(count > 0 for count in counts):
                    return True
            else:  # counts is an int
                if counts > 0:
                    return True
    return False


def parse_number(num_str: str) -> int:
    return int(num_str.replace(',', ''))

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
                test_name=test_name,
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

        current_operators = {}
        for op_name, count in operator_matches:
            current_operators[op_name] = int(count)
            if op_name not in current_test.operators:
                current_test.operators[op_name] = []

        for op_name in current_test.operators:
            current_test.operators[op_name].append(current_operators.get(op_name, 0))

        # Update is_packed definition to check for INLJ_PACKED
        current_test.is_packed = 1 if has_packed_inlj(current_operators) else 0

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

        time_match = re.findall(r'Execution time: (\d+) us', section)
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
            run_count = 0

    if current_test is not None:
        tests.append(current_test)

    return tests

def format_memory(memory_mb: float) -> str:
    return f"{memory_mb:.2f}MB"

def create_table(title: str) -> PrettyTable:
    table = PrettyTable()
    table.title = title
    table.field_names = [
        "Test",
        "Query",
        "Column Order",
        "Operators",
        "Instructions",
        "Cycles",
        "Branch Misses",
        "Cache Misses",
        "Page Faults",
        "Memory",
        "Time(ms)",
        "Speedup",
        "Equivalent Query"
    ]

    table.padding_width = 1
    table.hrules = 0
    table.vrules = 1

    for field in table.field_names:
        table.align[field] = "c"

    return table

def format_operators(operators: Dict[str, List[int]]) -> str:
    def operator_sort_key(op_name):
        if op_name == 'SCAN':
            return (0, '')
        elif op_name.startswith('INLJ_PACKED'):
            num = re.search(r'\d+', op_name)
            return (2, int(num.group()) if num else 0)
        elif op_name.startswith('INLJ'):
            num = re.search(r'\d+', op_name)
            return (1, int(num.group()) if num else 0)
        elif op_name == 'SINK_NO_OP':
            return (3, '')
        elif op_name == 'SINK_PACKED':
            return (4, '')
        elif op_name == 'SINK':
            return (5, '')
        return (6, op_name)

    sorted_ops = sorted(operators.items(), key=lambda x: operator_sort_key(x[0]))
    op_values = [sum(counts) // len(counts) for _, counts in sorted_ops]
    return op_values

def are_queries_equivalent(query1: TestMetrics, query2: TestMetrics) -> bool:
    """Check if two queries are equivalent (same query and ordering)"""
    return (query1.query == query2.query and
            query1.column_ordering == query2.column_ordering)

def get_sink_type(operators: Union[Dict[str, List[int]], Dict[str, int]]) -> str:
    """
    Determine the type of sink operator being used.
    Returns: 'no_op', 'packed', 'regular', or None
    """
    if isinstance(next(iter(operators.values())), list):
        # Handle List[int] case - use max value to determine if operator was used
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

def calculate_speedups(packed_tests: List[TestMetrics], base_tests: List[TestMetrics]):
    """
    Calculate speedup for packed queries compared to their unpacked counterparts.
    Matches queries based on compatible execution patterns.
    """
    print ([test.test_name for test in base_tests])
    for packed_test in packed_tests:
        # Find matching unpacked tests with same query and ordering
        matching_base_tests = [
            test for test in base_tests
            if (are_queries_equivalent(test, packed_test) and    # Same query and ordering
                are_execution_patterns_compatible(packed_test, test))  # Compatible sink types
        ]

        if matching_base_tests:
            # Use the first matching base test
            base_test = matching_base_tests[0]
            base_time = statistics.median(base_test.elapsed_time)
            packed_time = statistics.median(packed_test.elapsed_time)

            if packed_time > 0:
                packed_test.speedup = base_time / packed_time
                packed_test.equivalent_query = f"Test {base_test.test_name}"


def add_row_to_table(table: PrettyTable, test: TestMetrics, max_query: int = 30, max_order: int = 20):
    query = test.query[:max_query] + ('...' if len(test.query) > max_query else '')
    column_order = test.column_ordering[:max_order] + ('...' if len(test.column_ordering) > max_order else '')

    row = [
        test.test_name,
        query,
        column_order,
        format_operators(test.operators),
        sum(test.instructions_atom) // 1,
        sum(test.cycles_atom) // 1,
        sum(test.branch_misses_atom) // 1,
        sum(test.cache_misses_atom) // 1,
        sum(test.page_faults) // 1,
        format_memory(statistics.median(test.peak_memory)),
        f"{statistics.median(test.elapsed_time):.0f}",
        f"{test.speedup:.2f}x" if test.speedup > 0 else "N/A",
        test.equivalent_query
    ]
    table.add_row(row)

def write_to_csv(tests: List[TestMetrics], output_path: str):
    """Write test results to a CSV file."""
    fieldnames = [
        'test_name',
        'query',
        'column_ordering',
        'operators',
        'instructions',
        'cycles',
        'branch_misses',
        'cache_misses',
        'page_faults',
        'peak_memory',
        'elapsed_time',
        'is_packed',
        'speedup',
        'equivalent_query'
    ]

    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for test in sorted(tests, key=lambda x: (x.is_packed, x.query, x.column_ordering)):
            row = {
                'test_name': test.test_name,
                'query': test.query,
                'column_ordering': test.column_ordering,
                'operators': format_operators(test.operators),
                'instructions': sum(test.instructions_atom) // 1,
                'cycles': sum(test.cycles_atom) // 1,
                'branch_misses': sum(test.branch_misses_atom) // 1,
                'cache_misses': sum(test.cache_misses_atom) // 1,
                'page_faults': sum(test.page_faults) // 1,
                'peak_memory': statistics.median(test.peak_memory),
                'elapsed_time': statistics.median(test.elapsed_time),
                'is_packed': test.is_packed,
                'speedup': test.speedup,
                'equivalent_query': test.equivalent_query
            }
            writer.writerow(row)

def print_tables(tests: List[TestMetrics]):
    import os
    try:
        terminal_size = os.get_terminal_size()
        PrettyTable._max_width = terminal_size.columns
    except:
        PrettyTable._max_width = 32767

    packed_tests = [test for test in tests if test.is_packed == 1]
    base_tests = [test for test in tests if test.is_packed == 0]

    packed_tests.sort(key=lambda x: (x.query, x.column_ordering))
    base_tests.sort(key=lambda x: (x.query, x.column_ordering))

# Calculate speedups for packed tests
    calculate_speedups(packed_tests, base_tests)

    packed_table = create_table("Packed Execution Results")
    for test in packed_tests:
        add_row_to_table(packed_table, test)

    base_table = create_table("Base Execution Results")
    for test in base_tests:
        add_row_to_table(base_table, test)

    print("\nPacked Execution Results:")
    print(packed_table)
    print("\nBase Execution Results:")
    print(base_table)

def write_to_json(tests: List[TestMetrics], output_path: str):
    """Write test results to a JSON file."""
    results = []

    for test in sorted(tests, key=lambda x: (x.is_packed, x.query, x.column_ordering)):
        results.append({
            'test_name': test.test_name,
            'query': test.query,
            'column_ordering': test.column_ordering,
            'operators': {op: counts for op, counts in test.operators.items()},
            'instructions': sum(test.instructions_atom) // 1,
            'cycles': sum(test.cycles_atom) // 1,
            'branch_misses': sum(test.branch_misses_atom) // 1,
            'cache_misses': sum(test.cache_misses_atom) // 1,
            'page_faults': sum(test.page_faults) // 1,
            'peak_memory': statistics.median(test.peak_memory),
            'elapsed_time': statistics.median(test.elapsed_time),
            'is_packed': test.is_packed,
            'speedup': test.speedup,
            'equivalent_query': test.equivalent_query
        })

    with open(output_path, 'w') as jsonfile:
        json.dump(results, jsonfile, indent=4)

def main():
    if len(sys.argv) not in [3, 4, 5]:
        print("Usage: python script.py <log_file_path> <num_runs> [csv_output_path]")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        content = f.read()

    num_runs = int(sys.argv[2])
    tests = parse_log_file(content, num_runs)

    # Print tables to console
    print_tables(tests)

    # Write to CSV if output path is provided
    if len(sys.argv) == 4:
        csv_output_path = sys.argv[3]
        write_to_csv(tests, csv_output_path)
        print(f"\nResults have been written to: {csv_output_path}")

    # Write to JSON if output path is provided
    if len(sys.argv) == 5:
        json_output_path = sys.argv[4]
        write_to_json(tests, json_output_path)
        print(f"\nResults have been written to JSON: {json_output_path}")

if __name__ == "__main__":
    main()

