//
// Created by sunny on 9/7/24.
//

#include <iostream>
#include <memory>
#include "../src/engine/include/pipeline.hh"
#include "../src/operator/include/index_nested_loop_join_operator.hh"
#include "../src/operator/include/relation_types.hh"
#include "../src/operator/include/scan_operator.hh"
#include "../src/operator/include/sink_operator.hh"
#include "../src/utils/include/debug_enabled.hh"

std::shared_ptr<VFEngine::Operator> create_operator_plan() {
    const std::string table = "R";
    const std::vector<std::string> columns = {"a", "b", "c"};
    const std::unordered_map<std::string, VFEngine::SchemaType> schema{
                {"a", VFEngine::FLAT},
                {"b", VFEngine::FLAT},
                {"c", VFEngine::UNFLAT}}; // a is flat because the operator plan we chose scans a first

    // Sink
    auto sink = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Sink>(schema));

    // Join2
    const std::string ip_attribute2 = "b";
    const std::string op_attribute2 = "c";
    bool is_join_index_fwd2 = false;
    VFEngine::RelationType relation_type2 = VFEngine::MANY_TO_MANY;
    auto extend2 = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::IndexNestedLoopJoin>(
            table, ip_attribute2, op_attribute2, is_join_index_fwd2, relation_type2, sink));


    // Join1
    const std::string ip_attribute1 = "a";
    const std::string op_attribute1 = "b";
    bool is_join_index_fwd1 = true;
    VFEngine::RelationType relation_type1 = VFEngine::MANY_TO_MANY;
    auto extend1 = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::IndexNestedLoopJoin>(
            table, ip_attribute1, op_attribute1, is_join_index_fwd1, relation_type1, extend2));

    // Scan
    const std::string scan_attribute = "a";
    auto scan = std::static_pointer_cast<VFEngine::Operator>(
            std::make_shared<VFEngine::Scan>(table, scan_attribute, extend1));

    return scan;
}

long pipeline_example() {
    std::vector<std::string> table_names{"R"}; // self joins for now
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}, {"c", "src"}};

    auto pipeline = std::make_shared<VFEngine::Pipeline>(table_names, table_to_column_map, column_alias_map);
    const auto first_op = create_operator_plan();
    pipeline->set_first_operator(first_op);

    pipeline->init();
    pipeline->debug();
    pipeline->execute();

    return VFEngine::Sink::get_total_row_size_if_materialized();
}


long test_4() {
    return pipeline_example();
}

int main () {
    const std::string datalog4 = "Q = R(a, b), R(c, b)";
    std::cout << "Test 4: " << datalog4 << std::endl;

    long expected_result_test_4  = 14;
    long actual_result_test_4 = test_4();

    if (actual_result_test_4 != expected_result_test_4) {
        std::cerr << "Test 4 failed: Expected " << expected_result_test_4 << " but got " << actual_result_test_4 << std::endl;
        return 1;
    }

    return 0;
}
