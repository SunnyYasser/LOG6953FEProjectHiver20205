#include <iostream>
#include <memory>
#include <unistd.h>
#include "src/engine/include/pipeline.hh"
#include "src/operator/include/index_nested_loop_join_operator.hh"
#include "src/operator/include/relation_types.hh"
#include "src/operator/include/scan_operator.hh"
#include "src/operator/include/sink_operator.hh"
#include "src/utils/include/debug_enabled.hh"

std::shared_ptr<VFEngine::Operator> create_operator_plan1() {

    const std::vector<std::string> columns = {"a", "b"};
    const std::unordered_map<std::string, VFEngine::SchemaType> schema{{"a", VFEngine::FLAT}, {"b", VFEngine::UNFLAT}};

    // Sink
    auto sink = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Sink>(schema));


    // Join1
    const std::string ip_attribute1 = "a";
    const std::string op_attribute1 = "b";
    bool is_join_index_fwd1 = true;
    VFEngine::RelationType relation_type1 = VFEngine::MANY_TO_MANY;
    auto extend1 = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::IndexNestedLoopJoin>(
            ip_attribute1, op_attribute1, is_join_index_fwd1, relation_type1, sink));

    // Scan
    const std::string scan_attribute = "a";
    auto scan = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Scan>(scan_attribute, extend1));

    return scan;
}

void pipeline_example1() {
    std::vector<std::string> column_names{"src", "dest"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
    const std::string datalog = "Q = R(a, b)";
    const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}};

    auto pipeline = std::make_shared<VFEngine::Pipeline>(column_names, column_alias_map);

    auto first_op = create_operator_plan1();
    pipeline->set_first_operator(first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();

    std::cout << "count (*) R(a, b) = " << VFEngine::Sink::get_total_row_size_if_materialized() << std::endl;
}

void enable_component_debug() {
    if (std::getenv("GETCHAR_IN_OPERATORS")) {
        enable_operator_debug();
    }

    if (std::getenv("GETCHAR_IN_MEMORY")) {
        enable_memory_debug();
    }
}

int main() {
    if (std::getenv("GETCHAR_IN_MAIN")) {
        enable_debug();
        std::cout << "Current process id: " << ::getpid() << std::endl;
        do {
            std::cout << '\n' << "Press a key to continue...";
        } while (std::cin.get() != '\n');
    }

    enable_component_debug();
    pipeline_example1();

    return 0;
}
