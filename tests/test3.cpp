//
// Created by sunny on 9/7/24.
//

#include <iostream>
#include <memory>
#include <testpaths.hh>

#include "../src/engine/include/pipeline.hh"
#include "../src/operator/include/index_nested_loop_join_operator.hh"
#include "../src/operator/include/relation_types.hh"
#include "../src/operator/include/scan_operator.hh"
#include "../src/operator/include/sink_operator.hh"

std::shared_ptr<VFEngine::Operator> create_operator_plan() {
    const std::string table = "R";
    const std::vector<std::string> columns = {"a", "b"};
    const std::unordered_map<std::string, VFEngine::SchemaType> schema{{"a", VFEngine::FLAT}, {"b", VFEngine::UNFLAT}};

    // Sink
    auto sink = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Sink>(schema));

    // Join1
    const std::string ip_attribute = "a";
    const std::string op_attribute = "b";
    bool is_join_index_fwd = true;
    VFEngine::RelationType relation_type = VFEngine::MANY_TO_MANY;

    auto extend = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::IndexNestedLoopJoin>(
            ip_attribute, op_attribute, is_join_index_fwd, relation_type, sink));

    // Scan
    const std::string scan_attribute = "a";
    auto scan = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Scan>(scan_attribute, extend));

    return scan;
}

long pipeline_example() {
    std::vector<std::string> column_names{"src", "dest"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};

    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}};
    auto pipeline = std::make_shared<VFEngine::Pipeline>(column_names, column_alias_map);

    const auto first_op = create_operator_plan();
    pipeline->set_first_operator(first_op);
    pipeline->init();
    pipeline->debug();
    pipeline->execute();

    return VFEngine::Sink::get_total_row_size_if_materialized();
}

long test_3() { return pipeline_example(); }

long get_expected_value() {
    if (get_amazon0601_csv_path()) {
        return 3387388;
    }
    return 6;
}

int main() {
    const std::string datalog3 = "Q = R(a, b)";
    std::cout << "Test 3: " << datalog3 << std::endl;

    long expected_result_test_3 = get_expected_value();
    long actual_result_test_3 = test_3();

    if (actual_result_test_3 != expected_result_test_3) {
        std::cerr << "Test 1 failed: Expected " << expected_result_test_3 << " but got " << actual_result_test_3
                  << std::endl;
        return 1;
    }

    return 0;
}
