#include <chrono>
#include <iostream>
#include <memory>
#include <sink_packed_operator.hh>
#include <sys/resource.h>
#include <testpaths.hh>

#include "../src/engine/include/pipeline.hh"
#include "../src/parser/include/query_parser.hh"
void print_column_ordering(const std::vector<std::string> &column_ordering) {
    std::cout << "COLUMN ORDERING: ";
    for (int i = 0; i < column_ordering.size(); i++) {
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

ulong pipeline_example(const std::string &query) {
    std::vector<std::string> column_names{"src", "dest"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}, {"c", "src"}};

    const std::vector<std::string> column_ordering = {"b", "a", "c"};
    print_column_ordering(column_ordering);

    const auto parser = std::make_unique<VFEngine::QueryParser>(
            query, column_ordering, true, VFEngine::SinkType::PACKED, column_names, column_alias_map);

    const auto pipeline = parser->build_physical_pipeline();
    pipeline->init();

    benchmark_barrier();
    exec_start_time = std::chrono::steady_clock::now();

    pipeline->execute();

    benchmark_barrier();
    exec_end_time = std::chrono::steady_clock::now();

    auto first_op = pipeline->get_first_operator();
    const std::vector<std::string> operator_names{"SCAN", "INLJ_PACKED1", "INLJ_PACKED2", "SINK_PACKED"};
    int idx = 0;

    while (first_op) {
        std::cout << operator_names[idx++] << " " << first_op->get_uuid() << " : " << first_op->get_exec_call_counter()
                  << std::endl;
        first_op = first_op->get_next_operator();
    }


    return VFEngine::SinkPacked::get_total_row_size_if_materialized();
}

ulong test(const std::string &query) { return pipeline_example(query); }

ulong get_expected_value() {
    if (get_amazon0601_csv_path()) {
        return 122605698;
    }
    return 14;
}


int main() {
    const std::string query = "a->b,c->b";
    std::cout << "Test 8: " << query << std::endl;
    const auto start = std::chrono::steady_clock::now();
    const auto expected_result_test = get_expected_value();
    const auto actual_result_test = test(query);
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
    std::cout << "Total time: " << duration.count() << " us" << std::endl;
    std::cout << "Execution time: " << exec_duration.count() << " us" << std::endl;

    if (actual_result_test != expected_result_test) {
        std::cerr << "Test 8 failed: Expected " << expected_result_test << " but got " << actual_result_test
                  << std::endl;
        return 1;
    }

    return 0;
}
