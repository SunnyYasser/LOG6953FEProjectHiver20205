#include <iostream>
#include <memory>
#include <sink_packed_operator.hh>
#include <unistd.h>
#include "src/engine/include/pipeline.hh"
#include "src/operator/include/index_nested_loop_join_operator.hh"
#include "src/operator/include/relation_types.hh"
#include "src/operator/include/scan_operator.hh"
#include "src/operator/include/sink_operator.hh"
#include "src/parser/include/factorized_tree.hh"
#include "src/parser/include/factorized_tree_element.hh"
#include "src/parser/include/query_parser.hh"
#include "src/utils/include/debug_enabled.hh"

std::shared_ptr<VFEngine::Operator> create_operator_plan1() {

    const std::vector<std::string> columns = {"a", "b"};
    const std::vector<std::pair<std::string, VFEngine::SchemaType>> schema{{"a", VFEngine::FLAT},
                                                                           {"b", VFEngine::UNFLAT}};

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
    std::cout << "Execute calls for different operators" << std::endl;

    const std::vector<std::string> operator_names{"SCAN", "INLJ", "SINK"};
    int idx = 0;

    while (first_op) {
        std::cout << operator_names[idx++] << " " << first_op->get_uuid() << " : " << first_op->get_exec_call_counter()
                  << std::endl;
        first_op = first_op->get_next_operator();
    }
}

void enable_component_debug() {
    if (std::getenv("GETCHAR_IN_OPERATOR")) {
        enable_operator_debug();
    }

    if (std::getenv("GETCHAR_IN_MEMORY")) {
        enable_memory_debug();
    }

    if (std::getenv("GETCHAR_IN_PARSER")) {
        enable_parser_debug();
    }
}

void parser_example() {
    const std::string datalog = "a->b,a->c,b->d,c->e";
    const std::vector<std::string> column_ordering = {"a", "b", "c", "d", "e"};
    const std::unordered_map<std::string, std::string> column_alias_map{
            {"a", "src"}, {"b", "src"}, {"c", "src"}, {"d", "dest"}, {"e", "dest"}};


    const std::vector<std::string> column_names{"src", "dest"};

    const auto parser =
            std::make_unique<VFEngine::QueryParser>(datalog, column_ordering, true, column_names, column_alias_map);

    const auto pipeline = parser->build_physical_pipeline();
    pipeline->init();
    pipeline->execute();

    std::cout << "count (*) " << datalog << " = " << VFEngine::SinkPacked::get_total_row_size_if_materialized()
              << std::endl;

    const auto parser2 =
        std::make_unique<VFEngine::QueryParser>(datalog, column_ordering, false, column_names, column_alias_map);

    const auto pipeline2 = parser2->build_physical_pipeline();
    pipeline2->init();
    pipeline2->execute();

    std::cout << "count (*) " << datalog << " = " << VFEngine::Sink::get_total_row_size_if_materialized()
              << std::endl;


    const auto& ftree = parser->create_factorized_tree();
    ftree->print_tree();
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
    parser_example();
    return 0;
}
