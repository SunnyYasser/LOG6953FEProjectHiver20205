import subprocess
import re
import os

# Function to run the perf command and capture the output
def run_perf(executable):
    try:
        # Run the perf command with desired options
        perf_command = [
            'perf', 'stat',
            '-e', 'cache-references,cache-misses,instructions,task-clock,cycles',
            executable
        ]

        # Execute the perf command and capture the output
        result = subprocess.run(perf_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        output = result.stderr  # perf outputs metrics to stderr by default
        
        return output

    except FileNotFoundError:
        print("Perf tool is not installed or not found.")
        return None

# Function to parse the perf output and extract the metrics
def parse_perf_output(output):
    # will add some instrumentation here as well
    print(output)

# Main execution
if __name__ == '__main__':

    print ("...Starting perf profiling")
    
    # Get the absolute path of the executable
    parent_dir = os.path.split(os.getcwd())[0]
    executable_name = "cmake-build-debug/mydb2"  

    final_exec_path = os.path.join (parent_dir, executable_name)

    # Run perf and get the output
    perf_output = run_perf(final_exec_path)

    # Parse the output and display the results
    if perf_output:
        parse_perf_output (perf_output)

