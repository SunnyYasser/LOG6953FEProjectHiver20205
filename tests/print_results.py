import re
from dataclasses import dataclass
from typing import List, Dict
import sys
from prettytable import PrettyTable
import statistics

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

        current_operators = {}
        for op_name, count in operator_matches:
            current_operators[op_name] = int(count)
            if op_name not in current_test.operators:
                current_test.operators[op_name] = []

        for op_name in current_test.operators:
            current_test.operators[op_name].append(current_operators.get(op_name, 0))

        current_test.is_packed = 1 if any(('PACKED' in op) for op in current_operators.keys()) else 0

        if instr_match := re.search(r'([\d,]+)\s+.*?instructions', section):
            current_test.instructions_atom.append(parse_number(instr_match.group(1)))

        if cycles_match := re.search(r'([\d,]+)\s+.*?cycles', section):
            current_test.cycles_atom.append(parse_number(cycles_match.group(1)))

        if branch_misses_match := re.search(r'([\d,]+)\s+.*?branch-misses', section):
            current_test.branch_misses_atom.append(parse_number(branch_misses_match.group(1)))

        if cache_misses_match := re.search(r'([\d,]+)\s+.*?cache-misses', section):
            current_test.cache_misses_atom.append(parse_number(cache_misses_match.group(1)))

        if faults_match := re.search(r'([\d,]+)\s+.*?page-faults', section):
            current_test.page_faults.append(parse_number(faults_match.group(1)))

        if time_match := re.findall(r'Execution time: (\d+) ms', section):
            for time in time_match:
                current_test.elapsed_time.append(float(time))

        if memory_match := re.findall(r'Peak Memory Usage: (\d+\.\d+)', section):
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
        "Time(ms)"
    ]

    table.padding_width = 1
    table.hrules = 0  # Remove all horizontal lines
    table.vrules = 1  # Keep vertical lines

    # Center align all fields
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
    return '[' + ' '.join(str(val) for val in op_values) + ']'

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
        f"{statistics.median(test.elapsed_time):.0f}"
    ]
    table.add_row(row)

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

def main():
    if len(sys.argv) != 3:
        print("Usage: python script.py <log_file_path> <num_runs>")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        content = f.read()

    num_runs = int(sys.argv[2])
    tests = parse_log_file(content, num_runs)
    print_tables(tests)

if __name__ == "__main__":
    main()
