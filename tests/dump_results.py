import re
from dataclasses import dataclass
from typing import List, Dict
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

def get_git_commit_hash():
    try:
        return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('ascii').strip()
    except:
        return "unknown"

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
        for op_name, count in operator_matches:
            if op_name not in current_test.operators:
                current_test.operators[op_name] = []
            current_test.operators[op_name].append(int(count))

        current_test.is_packed = 1 if any(('PACKED' in op) for op in current_test.operators.keys()) else 0

        if instr_match := re.search(r'([\d,]+)\s+.*?instructions', section):
            current_test.instructions_atom.append(parse_number(instr_match.group(1)))

        if cycles_match := re.search(r'([\d,]+)\s+.*?cycles', section):
            current_test.cycles_atom.append(parse_number(cycles_match.group(1)))

        if faults_match := re.search(r'([\d,]+)\s+.*?page-faults', section):
            current_test.page_faults.append(parse_number(faults_match.group(1)))

        if branch_misses_match := re.search(r'([\d,]+)\s+.*?branch-misses', section):
            current_test.branch_misses_atom.append(parse_number(branch_misses_match.group(1)))

        if cache_misses_match := re.search(r'([\d,]+)\s+.*?cache-misses', section):
            current_test.cache_misses_atom.append(parse_number(cache_misses_match.group(1)))

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

    if current_test is not None:
        tests.append(current_test)

    return tests

def write_experimental_results(tests: List[TestMetrics], experiment_name: str, output_path: str):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    commit_hash = get_git_commit_hash()
    experiment_id = f"{timestamp}_{commit_hash[:8]}"

    with open(f"{output_path}/experiments.csv", 'a', newline='') as f:
        writer = csv.writer(f)
        if f.tell() == 0:  # File is empty, write header
            writer.writerow(['experiment_id', 'experiment_name', 'timestamp', 'commit_hash', 'config'])
        writer.writerow([experiment_id, experiment_name, timestamp, commit_hash, json.dumps(None)])

    with open(f"{output_path}/queries.csv", 'a', newline='') as f:
        writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if f.tell() == 0:  # File is empty, write header
            writer.writerow(['experiment_id', 'query_id', 'ordering', 'is_packed', 'metrics'])

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
                json.dumps(metrics, ensure_ascii=False)
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
