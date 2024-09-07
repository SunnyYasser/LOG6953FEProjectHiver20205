#include <iostream>
#include <memory>
#include <unistd.h>
#include "engine/include/pipeline.hh"
#include "operator/include/index_nested_loop_join_operator.hh"
#include "operator/include/relation_types.hh"
#include "operator/include/scan_operator.hh"
#include "operator/include/sink_operator.hh"
#include "utils/include/debug_enabled.hh"

std::shared_ptr<VFEngine::Operator> create_operator_plan1() {

    const std::string table = "R";
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

std::shared_ptr<VFEngine::Operator> create_operator_plan2() {

    const std::string table = "R";
    const std::vector<std::string> columns = {"a", "b", "c"};
    const std::unordered_map<std::string, VFEngine::SchemaType> schema{
            {"a", VFEngine::FLAT}, {"b", VFEngine::FLAT}, {"c", VFEngine::UNFLAT}};

    // Sink
    auto sink = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Sink>(schema));

    // Join2
    const std::string ip_attribute2 = "b";
    const std::string op_attribute2 = "c";
    bool is_join_index_fwd2 = true;
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

std::shared_ptr<VFEngine::Operator> create_operator_plan3() {

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
            table, ip_attribute, op_attribute, is_join_index_fwd, relation_type, sink));

    // Scan
    const std::string scan_attribute = "a";
    auto scan = std::static_pointer_cast<VFEngine::Operator>(
            std::make_shared<VFEngine::Scan>(table, scan_attribute, extend));

    return scan;
}

std::shared_ptr<VFEngine::Operator> create_operator_plan4() {

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


void pipeline_example1() {
    std::vector<std::string> table_names{"R"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::string datalog = "Q = R(a, b), R(a, c)";
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}, {"c", "dest"}};

    auto pipeline = std::make_shared<VFEngine::Pipeline>(table_names, table_to_column_map, column_alias_map);

    auto first_op = create_operator_plan1();
    pipeline->set_first_operator(first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();
}

void pipeline_example2() {
    std::vector<std::string> table_names{"R"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::string datalog = "Q = R(a, b), R(b, c)";
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "src,dest"}, {"c", "dest"}};

    auto pipeline = std::make_shared<VFEngine::Pipeline>(table_names, table_to_column_map, column_alias_map);

    auto first_op = create_operator_plan2();
    pipeline->set_first_operator(first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();
}

void pipeline_example3() {
    std::vector<std::string> table_names{"R"}; // self joins for now
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::string datalog = "Q = R(a, b)";
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}};

    auto pipeline = std::make_shared<VFEngine::Pipeline>(table_names, table_to_column_map, column_alias_map);

    const auto first_op = create_operator_plan3();
    pipeline->set_first_operator(first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();
}

void pipeline_example4() {
    std::vector<std::string> table_names{"R"}; // self joins for now
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::string datalog = "Q = R(a, b), R(c, b)";
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}, {"c", "src"}};

    auto pipeline = std::make_shared<VFEngine::Pipeline>(table_names, table_to_column_map, column_alias_map);

    const auto first_op = create_operator_plan4();
    pipeline->set_first_operator(first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();
}


int main() {
    if (std::getenv("GETCHAR_IN_MAIN")) {
        enable_debug();
        std::cout << "Current process id: " << ::getpid() << std::endl;
        do {
            std::cout << '\n' << "Press a key to continue...";
        } while (std::cin.get() != '\n');
    }

    pipeline_example1();
    //pipeline_example2();
    //pipeline_example3();
    //pipeline_example4();


    return 0;
}
