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

    const std::vector<std::string> columns = {"a", "b", "c"};
    const std::unordered_map<std::string, VFEngine::SchemaType> schema{
            {"a", VFEngine::FLAT}, {"b", VFEngine::UNFLAT}, {"c", VFEngine::UNFLAT}};

    // Sink
    auto sink = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Sink>(schema));

    // Join2
    const std::string ip_attribute2 = "a";
    const std::string op_attribute2 = "c";
    bool is_join_index_fwd2 = true;
    VFEngine::RelationType relation_type2 = VFEngine::MANY_TO_MANY;
    auto extend2 = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::IndexNestedLoopJoin>(
            ip_attribute2, op_attribute2, is_join_index_fwd2, relation_type2, sink));


    // Join1
    const std::string ip_attribute1 = "a";
    const std::string op_attribute1 = "b";
    bool is_join_index_fwd1 = true;
    VFEngine::RelationType relation_type1 = VFEngine::MANY_TO_MANY;
    auto extend1 = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::IndexNestedLoopJoin>(
            ip_attribute1, op_attribute1, is_join_index_fwd1, relation_type1, extend2));

    // Scan
    const std::string scan_attribute = "a";
    auto scan = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Scan>(scan_attribute, extend1));

    return scan;
}

long pipeline_example() {
    std::vector<std::string> column_names{"src", "dest"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}, {"c", "dest"}};

    auto pipeline = std::make_shared<VFEngine::Pipeline>(column_names, column_alias_map);
    auto first_op = create_operator_plan();
    pipeline->set_first_operator(first_op);

    pipeline->init();
    pipeline->debug();
    pipeline->execute();

    return VFEngine::Sink::get_total_row_size_if_materialized();
}


long test_1() { return pipeline_example(); }

long get_expected_value () {
    if (get_amazon0601_csv_path ()) {
        return 31583974;
    }
    return 14;
}

int main() {
    const std::string datalog1 = "Q = R(a, b), R(a, c)";
    std::cout << "Test 1: " << datalog1 << std::endl;
    long expected_result_test_1 = get_expected_value ();
    long actual_result_test_1 = test_1();

    if (actual_result_test_1 != expected_result_test_1) {
        std::cerr << "Test 1 failed: Expected " << expected_result_test_1 << " but got " << actual_result_test_1
                  << std::endl;
        return 1;
    }

    return 0;
}
