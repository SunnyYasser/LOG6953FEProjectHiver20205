#include <chrono>
#include <iostream>
#include <memory>
#include "src/operator/include/sink_packed_operator.hh"
#include <sys/resource.h>
#include "src/engine/include/pipeline.hh"
#include "src/operator/include/sink_operator.hh"
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

ulong run_pipeline(const std::string &dataset_path, const std::string &serialized_dataset_path,
                   const std::string &query, const std::vector<std::string> &column_ordering, const bool &is_packed) {
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
            query, column_ordering, is_packed, is_packed ? VFEngine::SinkType::PACKED : VFEngine::SinkType::UNPACKED,
            column_names, column_alias_map);

    VFEngine::DataSourceTable::set_dataset_path(dataset_path);
    VFEngine::DataSourceTable::set_serialized_dataset_path(serialized_dataset_path);
    const auto pipeline = parser->build_physical_pipeline();
    pipeline->init();

    benchmark_barrier();
    exec_start_time = std::chrono::steady_clock::now();

    pipeline->execute();

    benchmark_barrier();
    exec_end_time = std::chrono::steady_clock::now();

    auto first_op = pipeline->get_first_operator();

    std::vector<std::string> operator_names;
    operator_names.emplace_back("SCAN");
    for (int i = 0; i < static_cast<int>(column_ordering.size()) - 1; i++) {
        if (is_packed) {
            operator_names.push_back("INLJ_PACKED" + std::to_string(i + 1));
        } else {
            operator_names.push_back("INLJ" + std::to_string(i + 1));
        }
    }
    operator_names.emplace_back("SINK");

    int idx = 0;
    while (first_op) {
        std::cout << operator_names[idx++] << " " << first_op->get_uuid() << " : " << first_op->get_exec_call_counter()
                  << std::endl;
        first_op = first_op->get_next_operator();
    }

    return is_packed ? VFEngine::SinkPacked::get_total_row_size_if_materialized()
                     : VFEngine::Sink::get_total_row_size_if_materialized();
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

int execute(const std::string &dataset_path, const std::string &serialized_dataset_path, const std::string &query,
            const std::string &column_ordering, const ulong &expected_result, const bool &is_packed = false) {
    std::cout << "Executed Query: " << query << std::endl;
    const auto start = std::chrono::steady_clock::now();
    const std::vector<std::string> column_ordering_vector = split(column_ordering, ',');
    const auto actual_result =
            run_pipeline(dataset_path, serialized_dataset_path, query, column_ordering_vector, is_packed);
    const auto end = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    const auto exec_duration = std::chrono::duration_cast<std::chrono::microseconds>(exec_end_time - exec_start_time);

    struct rusage usage;
    // Get resource usage
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        double peak_memory_mb = usage.ru_maxrss / 1024.0;
        printf("Peak Memory Usage: %.2f MB\n", peak_memory_mb);
    } else {
        printf("Peak Memory Usage: %d MB\n", -1);
    }

    std::cout << "Total Time: " << duration.count() << " us" << std::endl;
    std::cout << "Execution Time: " << exec_duration.count() << " us" << std::endl;
    std::cout << "Actual Result: " << actual_result << std::endl;
    std::cout << "Expected Result: " << expected_result << std::endl;
    const auto is_valid = actual_result == expected_result;
    std::cout << "Is Valid: " << is_valid << std::endl;

    if (actual_result != expected_result) {
        std::cerr << "Execution failed: Expected " << expected_result << " but got " << actual_result << std::endl;
        return 1;
    }

    return 0;
}

int main(const int argc, const char *argv[]) {
    if (argc != 6 && argc != 7) {
        std::cerr << "Usage: " << argv[0]
                  << " <dataset_path> <serialized_dataset_path> <query> <column_ordering> <expected_result> [is_packed]"
                  << std::endl;
        return 1;
    }
    const bool is_packed = argc == 7 ? std::stoi(argv[6]) : false;

    return execute(argv[1], argv[2], argv[3], argv[4], std::stoul(argv[5]), is_packed);
}
