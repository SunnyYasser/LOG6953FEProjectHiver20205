import re
from dataclasses import dataclass
from typing import List, Dict
import sys
from prettytable import PrettyTable
from operator import attrgetter

@dataclass
class TestMetrics:
    test_name: str
    query: str
    column_ordering: str
    operators: Dict[str, int]
    instructions_atom: int
    cycles_atom: int
    page_faults: int
    elapsed_time: float
    peak_memory: float
    is_packed: int

def parse_number(num_str: str) -> int:
    return int(num_str.replace(',', ''))

def parse_log_file(content: str) -> List[TestMetrics]:
    tests = []
    test_sections = re.split(r'Running test:', content)[1:]

    for section in test_sections:
        test_path_match = re.search(r'(.+?) with profiling', section)
        if not test_path_match:
            continue
        test_path = test_path_match.group(1).strip()
        test_name = test_path.split('/')[-1]

        query_match = re.search(r'Test \d+: ([^\n]+)', section)
        query = query_match.group(1) if query_match else "N/A"

        # Parse column ordering
        column_ordering_match = re.search(r'COLUMN ORDERING: ([^\n]+)', section)
        column_ordering = column_ordering_match.group(1) if column_ordering_match else "N/A"

        test_num_match = re.search(r'test(\d+)', test_name)
        test_num = test_num_match.group(1) if test_num_match else "N/A"

        metrics = TestMetrics(
            test_name=test_num,
            query=query,
            column_ordering=column_ordering,
            operators={},
            instructions_atom=0,
            cycles_atom=0,
            page_faults=0,
            elapsed_time=0.0,
            peak_memory=0.0,
            is_packed=0
        )

        operator_lines = re.findall(r'(?:SCAN|INLJ\d+|INLJ_PACKED\d+|SINK(?:_PACKED)?).*?: (\d+)', section)
        operator_types = re.findall(r'(SCAN|INLJ\d+|INLJ_PACKED\d+|SINK(?:_PACKED)?)', section)

        for op_type, count in zip(operator_types, operator_lines):
            if op_type.startswith('INLJ_PACKED'):
                metrics.operators['INLJ_PACKED'] = metrics.operators.get('INLJ_PACKED', 0) + int(count)
            elif op_type.startswith('INLJ'):
                metrics.operators['INLJ'] = metrics.operators.get('INLJ', 0) + int(count)
            elif op_type == 'SINK_PACKED':
                metrics.operators['SINK_PACKED'] = metrics.operators.get('SINK_PACKED', 0) + int(count)
            elif op_type == 'SINK':
                metrics.operators['SINK'] = metrics.operators.get('SINK', 0) + int(count)
            elif op_type == 'SCAN':
                metrics.operators['SCAN'] = metrics.operators.get('SCAN', 0) + int(count)

        metrics.is_packed = 1 if metrics.operators.get('SINK_PACKED', 0) > 0 else 0

        instr_match = re.search(r'([\d,]+)\s+cpu_atom/instructions/', section)
        if instr_match:
            metrics.instructions_atom = parse_number(instr_match.group(1))

        cycles_match = re.search(r'([\d,]+)\s+cpu_atom/cycles/', section)
        if cycles_match:
            metrics.cycles_atom = parse_number(cycles_match.group(1))

        faults_match = re.search(r'([\d,]+)\s+page-faults', section)
        if faults_match:
            metrics.page_faults = parse_number(faults_match.group(1))

        time_match = re.search(r'Test time =\s*(\d+\.\d+)', section)
        if time_match:
            metrics.elapsed_time = float(time_match.group(1))

        memory_match = re.search(r'Peak Memory Usage: (\d+\.\d+)', section)
        if memory_match:
            metrics.peak_memory = float(memory_match.group(1))

        tests.append(metrics)
    return tests

def format_memory(memory_mb: float) -> str:
    if memory_mb >= 1024:
        return f"{memory_mb/1024:.2f} GB"
    else:
        return f"{memory_mb:.2f} MB"

def create_table(title: str) -> PrettyTable:
    table = PrettyTable()
    table.title = title
    table.field_names = [
        "TestNum",
        "Query",
        "ColumnOrder",
        "SCAN",
        "INLJ",
        "INLJ_PACKED",
        "SINK",
        "SINK_PACKED",
        "Instructions",
        "Cycles",
        "PageFaults",
        "Memory",
        "Time(s)"
    ]

    table.padding_width = 0

    for field in table.field_names:
        table.align[field] = "r"
    table.align["Query"] = "l"
    table.align["ColumnOrder"] = "l"

    return table

def add_row_to_table(table: PrettyTable, test: TestMetrics, max_query: int = 20, max_order: int = 20):
    query = test.query[:max_query] + ('...' if len(test.query) > max_query else '')
    column_order = test.column_ordering[:max_order] + ('...' if len(test.column_ordering) > max_order else '')

    row = [
        test.test_name,
        query,
        column_order,
        test.operators.get('SCAN', 0),
        test.operators.get('INLJ', 0),
        test.operators.get('INLJ_PACKED', 0),
        test.operators.get('SINK', 0),
        test.operators.get('SINK_PACKED', 0),
        test.instructions_atom,
        test.cycles_atom,
        test.page_faults,
        format_memory(test.peak_memory),
        f"{test.elapsed_time:.2f}"
    ]
    table.add_row(row)

def print_tables(tests: List[TestMetrics]):
    # Split tests into packed and base
    packed_tests = [test for test in tests if test.is_packed == 1]
    base_tests = [test for test in tests if test.is_packed == 0]

    # Sort both lists by query first, then column ordering
    packed_tests.sort(key=lambda x: (x.query, x.column_ordering))
    base_tests.sort(key=lambda x: (x.query, x.column_ordering))

    # Create and populate packed table
    packed_table = create_table("Packed Execution Results")
    for test in packed_tests:
        add_row_to_table(packed_table, test)

    # Create and populate base table
    base_table = create_table("Base Execution Results")
    for test in base_tests:
        add_row_to_table(base_table, test)

    # Print both tables with spacing
    print("\nPacked Execution Results:")
    print(packed_table)
    print("\nBase Execution Results:")
    print(base_table)

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <log_file_path>")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        content = f.read()

    tests = parse_log_file(content)
    print_tables(tests)

if __name__ == "__main__":
    main()