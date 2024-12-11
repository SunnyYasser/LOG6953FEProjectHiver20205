#include "include/parser_debug.hh"
#include <iostream>
#include "../parser/include/query_parser.hh"
#include "../utils/include/debug_enabled.hh"
#include "include/operator_debug.hh"


namespace VFEngine {
    void ParserDebugUtility::print_logical_plan(const QueryParser *parser) {
        if (!is_parser_debug_enabled()) {
            return;
        }
        auto logical_pipeline = parser->get_logical_pipeline();
        for (auto &[operator_type, first_col, second_col, join_direction, relation_type]: logical_pipeline) {
            std::cout << "======== \n";
            std::cout << first_col << " , " << second_col << std::endl;
            std::cout << OperatorDebugUtility::get_operator_type_as_string(operator_type) << std::endl;
            std::cout << "======== \n";
        }
    }
} // namespace VFEngine
