#include <iostream>
#include <memory>
#include <testpaths.hh>

#include "../src/engine/include/pipeline.hh"
#include "../src/operator/include/sink_operator.hh"
#include "../src/parser/include/query_parser.hh"

ulong pipeline_example(const std::string &query) {
    std::vector<std::string> column_names{"src", "dest"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "src,dest"}, {"c", "dest"}};
    const std::vector<std::string> column_ordering = {"a", "b", "c"};

    const auto parser =
            std::make_unique<VFEngine::QueryParser>(query, column_ordering, false, column_names, column_alias_map);

    const auto pipeline = parser->build_physical_pipeline();
    pipeline->init();
    pipeline->execute();

    auto first_op = pipeline->get_first_operator();
    const std::vector<std::string> operator_names{"SCAN", "INLJ1", "INLJ2", "SINK"};
    int idx = 0;

    while (first_op) {
        std::cout << operator_names[idx++] << " " << first_op->get_uuid() << " : " << first_op->get_exec_call_counter()
                  << std::endl;
        first_op = first_op->get_next_operator();
    }

    return VFEngine::Sink::get_total_row_size_if_materialized();
}

ulong test_2(const std::string &query) { return pipeline_example(query); }

ulong get_expected_value() {
    if (get_amazon0601_csv_path()) {
        return 32373599;
    }
    return 4;
}

int main() {
    const std::string query = "a->b,b->c";
    std::cout << "Test 2: " << query << std::endl;

    const auto expected_result_test_2 = get_expected_value();
    const auto actual_result_test_2 = test_2(query);

    if (actual_result_test_2 != expected_result_test_2) {
        std::cerr << "Test 2 failed: Expected " << expected_result_test_2 << " but got " << actual_result_test_2
                  << std::endl;
        return 1;
    }

    return 0;
}
