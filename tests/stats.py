import re
from dataclasses import dataclass
from typing import List, Dict
import sys
from prettytable import PrettyTable

@dataclass
class TestMetrics:
    test_name: str
    operators: Dict[str, int]
    instructions_atom: int
    cycles_atom: int
    page_faults: int
    elapsed_time: float
    peak_memory: float

def parse_number(num_str: str) -> int:
    """Convert a string number with possible commas to integer."""
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

        metrics = TestMetrics(
            test_name=test_name,
            operators={},
            instructions_atom=0,
            cycles_atom=0,
            page_faults=0,
            elapsed_time=0.0,
            peak_memory=0.0
        )

        # Parse operators using exact matching
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

        # Updated patterns to handle comma-separated numbers
        instr_match = re.search(r'([\d,]+)\s+cpu_atom/instructions/', section)
        if instr_match:
            metrics.instructions_atom = parse_number(instr_match.group(1))

        cycles_match = re.search(r'([\d,]+)\s+cpu_atom/cycles/', section)
        if cycles_match:
            metrics.cycles_atom = parse_number(cycles_match.group(1))

        faults_match = re.search(r'([\d,]+)\s+page-faults', section)
        if faults_match:
            metrics.page_faults = parse_number(faults_match.group(1))

        # Parse time and memory
        time_match = re.search(r'Test time =\s*(\d+\.\d+)', section)
        if time_match:
            metrics.elapsed_time = float(time_match.group(1))

        memory_match = re.search(r'Peak Memory Usage: (\d+\.\d+)', section)
        if memory_match:
            metrics.peak_memory = float(memory_match.group(1))

        tests.append(metrics)
    return tests

def print_table(tests: List[TestMetrics]):
    table = PrettyTable()
    table.field_names = [
        "Test Name",
        "SCAN Calls",
        "INLJ Calls",
        "INLJ Packed Calls",
        "SINK Calls",
        "SINK Packed Calls",
        "CPU Instructions",
        "CPU Cycles",
        "Page Faults",
        "Peak Memory",
        "Test Time"
    ]

    # Set alignment for all columns
    for field in table.field_names:
        table.align[field] = "r"
    table.align["Test Name"] = "l"

    # Make the table more compact
    table.padding_width = 1

    for test in tests:
        row = [
            test.test_name[:25],
            test.operators.get('SCAN', 0),
            test.operators.get('INLJ', 0),
            test.operators.get('INLJ_PACKED', 0),
            test.operators.get('SINK', 0),
            test.operators.get('SINK_PACKED', 0),
            test.instructions_atom,
            test.cycles_atom,
            test.page_faults,
            f"{test.peak_memory:.2f} MB",
            f"{test.elapsed_time:.2f} sec"
        ]
        table.add_row(row)

    print(table)

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <log_file_path>")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        content = f.read()

    tests = parse_log_file(content)
    print_table(tests)

if __name__ == "__main__":
    main()