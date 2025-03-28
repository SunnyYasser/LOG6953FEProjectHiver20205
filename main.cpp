#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <set>
#include <sys/resource.h>
#include "src/engine/include/pipeline.hh"
#include "src/operator/include/sink_failure_prop.hh"
#include "src/parser/include/query_parser.hh"

void print_column_ordering(const std::vector<std::string> &column_ordering) {
    std::cout << "COLUMN ORDERING: ";
    for (size_t i = 0; i < column_ordering.size(); i++) {
        std::cout << column_ordering[i];
        if (i != column_ordering.size() - 1) {
            std::cout << ", ";
        }
    }
    std::cout << std::endl;
}

inline void benchmark_barrier() {
    std::atomic_thread_fence(std::memory_order_seq_cst); // prevent hardware reordering
    asm volatile("" ::: "memory"); // prevent compiler reordering (only for gcc)
}

std::chrono::steady_clock::time_point exec_start_time;
std::chrono::steady_clock::time_point exec_end_time;

std::vector<uint64_t> run_pipeline(const std::string &dataset_path, const std::string &serialized_dataset_path,
                                   const std::string &query, const std::vector<std::string> &column_ordering,
                                   const std::vector<uint64_t> &src_nodes) {
    std::vector<std::string> column_names{"src", "dest"};

    std::unordered_map<std::string, std::string> column_alias_map;
    for (int i = 0; i < static_cast<int>(column_ordering.size()); i++) {
        if (i == 0) {
            column_alias_map[column_ordering[i]] = column_names[0];
        } else {
            column_alias_map[column_ordering[i]] = column_names[1];
        }
    }

    print_column_ordering(column_ordering);
    const auto parser = std::make_unique<VFEngine::QueryParser>(
            query, column_ordering, true, src_nodes, VFEngine::SinkType::FAILURE_PROP, column_names, column_alias_map);

    VFEngine::DataSourceTable::set_dataset_path(dataset_path);
    VFEngine::DataSourceTable::set_serialized_dataset_path(serialized_dataset_path);
    const auto pipeline = parser->build_physical_pipeline();
    pipeline->init();

    benchmark_barrier();
    exec_start_time = std::chrono::steady_clock::now();

    pipeline->execute();

    benchmark_barrier();
    exec_end_time = std::chrono::steady_clock::now();

    auto current_op = pipeline->get_first_operator();
    std::shared_ptr<VFEngine::Operator> sink_op = nullptr;

    std::vector<std::string> operator_names;
    operator_names.emplace_back("SCAN");
    for (int i = 0; i < static_cast<int>(column_ordering.size()) - 1; i++) {
        operator_names.push_back("INLJ_PACKED" + std::to_string(i + 1));
    }
    operator_names.emplace_back("SINK");

    int idx = 0;
    while (current_op) {
        std::cout << operator_names[idx++] << " " << current_op->get_uuid() << " : "
                  << current_op->get_exec_call_counter() << std::endl;
        const auto next_op = current_op->get_next_operator();
        if (!next_op) {
            sink_op = current_op;
        }
        current_op = next_op;
    }

    const auto sink_failure_prop = std::static_pointer_cast<VFEngine::SinkFailureProp>(sink_op);
    return *sink_failure_prop->get_total_rows();
}

std::vector<std::string> split(const std::string &str, char delimiter) {
    std::vector<std::string> result;
    size_t start = 0, end;

    while ((end = str.find(delimiter, start)) != std::string::npos) {
        result.push_back(str.substr(start, end - start));
        start = end + 1;
    }
    result.push_back(str.substr(start));

    return result;
}

void deleteFileIfExists(const std::string &filename) {
    namespace fs = std::filesystem;

    if (fs::exists(filename)) {
        try {
            fs::remove(filename);
            std::cout << "File " << filename << " was deleted successfully." << std::endl;
        } catch (const fs::filesystem_error &e) {
            std::cerr << "Error deleting file: " << e.what() << std::endl;
        }
    } else {
        std::cout << "File " << filename << " does not exist." << std::endl;
    }
}

std::vector<std::vector<uint64_t>> execute(const std::string &dataset_path, const std::string &serialized_dataset_path,
                                           const std::vector<std::string> &queries,
                                           const std::vector<std::string> &column_orderings,
                                           const std::vector<uint64_t> &src_nodes_failure_prop,
                                           const std::string &output_stats_filename) {

    assert(queries.size() == column_orderings.size());
    std::vector<std::vector<uint64_t>> result;
    result.push_back(src_nodes_failure_prop);
    deleteFileIfExists(output_stats_filename);
    for (size_t i = 0; i < column_orderings.size(); i++) {
        const auto &query = queries[i];
        const auto &column_ordering = column_orderings[i];
        std::cout << "Executed Query: " << query << std::endl;
        std::cout << "Executed Ordering: " << column_ordering << std::endl;
        const auto start = std::chrono::steady_clock::now();
        const std::vector<std::string> column_ordering_vector = split(column_ordering, ',');
        const auto actual_result = run_pipeline(dataset_path, serialized_dataset_path, query, column_ordering_vector,
                                                src_nodes_failure_prop);
        const auto end = std::chrono::steady_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        const auto exec_duration =
                std::chrono::duration_cast<std::chrono::microseconds>(exec_end_time - exec_start_time);

        struct rusage usage;
        double peak_memory_mb = 0;
        // Get resource usage
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            peak_memory_mb = usage.ru_maxrss / 1024.0;
            printf("Peak Memory Usage: %.2f MB\n", peak_memory_mb);
        } else {
            printf("Peak Memory Usage: %d MB\n", -1);
        }

        std::cout << "Total Time: " << duration.count() << " ms" << std::endl;
        std::cout << "Execution Time: " << exec_duration.count() << " ms" << std::endl;

        // Write result to TachosDB_stats.txt
        std::ofstream output_file(output_stats_filename, std::ios::app);
        if (!output_file.is_open()) {
            std::cerr << "Failed to open " << output_stats_filename << " for writing" << std::endl;
            exit(-1);
        }

        output_file << "Total Time: " << duration.count() << " us" << std::endl;
        output_file << "Execution Time: " << exec_duration.count() << " us" << std::endl;
        output_file << "Peak Memory: " << std::fixed << std::setprecision(2) << peak_memory_mb << " MB" << std::endl;
        output_file.close();

        auto sorted_unique_vals = std::set(actual_result.begin(), actual_result.end());
        result.emplace_back(sorted_unique_vals.begin(), sorted_unique_vals.end());
    }
    return result;
}

int main() {
    const std::string dataset_path = "../data.txt";
    const std::string serialized_dataset_path = "../amazon0601";
    const std::vector<std::string> queries = {"a->b", "a->b,b->c", "a->b,b->c,c->d", "a->b,b->c,c->d,d->e"};
    const std::vector<std::string> column_orderings = {"a,b", "a,b,c", "a,b,c,d", "a,b,c,d,e"};
    const auto input_filename = "../input_query.txt";
    const auto output_filename = "../TachosDB_output.txt";
    const auto output_stats_filename = "../TachosDB_statistics.txt";

    // Read input_query.txt file
    std::ifstream input_file(input_filename);
    if (!input_file.is_open()) {
        std::cerr << "Failed to open input_query.txt" << std::endl;
        return 1;
    }

    std::string line;
    std::getline(input_file, line);
    input_file.close();

    // Validate input format
    // Check if the string starts with '[' and ends with ']'
    if (line.empty() || line.front() != '[' || line.back() != ']') {
        std::cerr << "Invalid input format in input_query.txt. Expected format: [1, 2, 3]" << std::endl;
        return 1;
    }

    // Parse the vector from the input
    std::vector<uint64_t> src_nodes_failure_prop;
    std::string content = line.substr(1, line.size() - 2); // Remove brackets

    std::stringstream ss(content);
    std::string item;

    // Parse comma-separated values
    while (std::getline(ss, item, ',')) {
        // Trim whitespace
        item.erase(0, item.find_first_not_of(" \t"));
        item.erase(item.find_last_not_of(" \t") + 1);

        // Check if the item is a valid integer
        bool valid = true;
        for (char c: item) {
            if (!std::isdigit(c)) {
                valid = false;
                break;
            }
        }

        if (!valid || item.empty()) {
            std::cerr << "Invalid number in input_file: '" << item << "'" << std::endl;
            return 1;
        }

        src_nodes_failure_prop.push_back(static_cast<uint64_t>(std::stoi(item)));
    }

    std::vector<std::vector<uint64_t>> result =
            execute(dataset_path, serialized_dataset_path, queries, column_orderings, src_nodes_failure_prop,
                    output_stats_filename);

    // Write result to TachosDB_output.txt
    std::ofstream output_file(output_filename);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open TachosDB_output.txt for writing" << std::endl;
        return 1;
    }

    // Write each vector on a new line
    for (const auto &row: result) {
        bool first = true;
        output_file << "[";
        for (const auto &val: row) {
            if (!first) {
                output_file << ", ";
            }
            output_file << val;
            first = false;
        }
        output_file << "]\n";
    }

    output_file.close();

    return 0;
}
