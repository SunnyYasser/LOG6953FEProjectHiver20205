#include <chrono>
#include <iostream>
#include <memory>
#include <sys/resource.h>
#include <testpaths.hh>

#include "../src/engine/include/pipeline.hh"
#include "../src/operator/include/sink_operator.hh"
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

ulong pipeline_example(const std::string &query) {
    std::vector<std::string> column_names{"src", "dest"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::unordered_map<std::string, std::string> column_alias_map{
            {"a", "src"}, {"b", "src"}, {"c", "src"}, {"d", "dest"}};

    const std::vector<std::string> column_ordering = {"a", "b", "c", "d"};
    print_column_ordering(column_ordering);

    const auto parser = std::make_unique<VFEngine::QueryParser>(query, column_ordering, VFEngine::SinkType::UNPACKED,
                                                                column_names, column_alias_map);

    const auto pipeline = parser->build_physical_pipeline();
    pipeline->init();
    pipeline->execute();

    auto first_op = pipeline->get_first_operator();

    const std::vector<std::string> operator_names{"SCAN", "INLJ1", "INLJ2", "INLJ3", "SINK"};
    int idx = 0;

    while (first_op) {
        std::cout << operator_names[idx++] << " " << first_op->get_uuid() << " : " << first_op->get_exec_call_counter()
                  << std::endl;
        first_op = first_op->get_next_operator();
    }

    return VFEngine::Sink::get_total_row_size_if_materialized();
}


ulong test_12(const std::string &query) { return pipeline_example(query); }

ulong get_expected_value() {
    if (get_amazon0601_csv_path()) {
        return 314212869;
    }
    return 1;
}

int main() {
    const std::string query = "a->b,b->c,c->d";
    std::cout << "Test 12: " << query << std::endl;
    const auto start = std::chrono::high_resolution_clock::now();
    const auto expected_result_test_12 = get_expected_value();
    const auto actual_result_test_12 = test_12(query);
    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    struct rusage usage;
    // Get resource usage
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        double peak_memory_mb = usage.ru_maxrss / 1024.0;
        printf("Peak Memory Usage: %.2f MB\n", peak_memory_mb);
    } else {
        printf("Peak Memory Usage: %d MB\n", -1);
    }
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;
    if (actual_result_test_12 != expected_result_test_12) {
        std::cerr << "Test 12 failed: Expected " << expected_result_test_12 << " but got " << actual_result_test_12
                  << std::endl;
        return 1;
    }

    return 0;
}
