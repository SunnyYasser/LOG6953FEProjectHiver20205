#include "include/query_parser.hh"

#include <cassert>
#include <factorized_tree.hh>
#include <iostream>
#include <sink_packed_operator.hh>
#include <sstream>
#include "../operator/include/index_nested_loop_join_operator.hh"
#include "../operator/include/index_nested_loop_join_packed_operator.hh"
#include "../operator/include/operator_types.hh"
#include "../operator/include/scan_operator.hh"
#include "../operator/include/sink_operator.hh"

#ifndef NDEBUG
#define M_Assert(Expr, Msg) __M_Assert(#Expr, Expr, __FILE__, __LINE__, Msg)
#else
#define M_Assert(Expr, Msg) ;
#endif

void __M_Assert(const char *expr_str, bool expr, const char *file, int line, const char *msg) {
    if (!expr) {
        std::cerr << "Assert failed:\t" << msg << "\n"
                  << "Expected:\t" << expr_str << "\n"
                  << "Source:\t\t" << file << ", line " << line << "\n";
        abort();
    }
}


namespace VFEngine {

    QueryParser::QueryParser(const std::string &query, const std::vector<std::string> &column_ordering, bool is_packed,
                             const std::vector<std::string> &column_names,
                             const std::unordered_map<std::string, std::string> &column_alias_map) :
        _query(query), _column_ordering(column_ordering), _is_packed(is_packed), _delimiter("->"),
        _column_names(column_names), _column_alias_map(column_alias_map) {}

    static std::vector<std::string> split(const std::string &str, char delimiter) {
        std::vector<std::string> tokens;
        std::stringstream ss(str);
        std::string token;
        while (getline(ss, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }

    std::vector<std::pair<std::string, SchemaType>> QueryParser::create_schema() {
        /*
         * Mark initially all as unflat
         * Start from i=1 in the column ordering
         * For each parent, child pair, mark parent as flat
         * No need to work with the _direction_map
         */
        std::vector<std::pair<std::string, SchemaType>> schema;
        std::unordered_map<std::string, SchemaType> schema_map;

        for (size_t i = 1; i < _column_ordering.size(); i++) {
            const auto &column = _column_ordering[i];
            for (size_t j = 0; j < i; j++) {
                const auto &parent = _column_ordering[j];
                for (auto &nbrs: _direction_map[parent]) {
                    if (nbrs.first == column) {
                        schema_map[parent] = FLAT;
                        schema_map[column] = UNFLAT;
                        break;
                    }
                }
            }
        }

        for (const auto &column: _column_ordering) {
            if (schema_map.find(column) == schema_map.end()) {
                schema.emplace_back(column, UNFLAT);
            } else {
                schema.emplace_back(column, schema_map[column]);
            }
        }

        return schema;
    }

    void QueryParser::build_direction_map() {
        std::vector<std::string> joins = split(_query, ',');
        for (const auto &join: joins) {
            const size_t arrowPos = join.find(_delimiter);
            if (arrowPos != std::string::npos) {
                std::string from = join.substr(0, arrowPos);
                std::string to = join.substr(arrowPos + 2);

                _direction_map[from].emplace_back(to, FORWARD);
                _direction_map[to].emplace_back(from, BACKWARD);
            }
        }
    }

    void QueryParser::build_logical_pipeline() {
        build_direction_map();
        for (size_t i = 0; i < _column_ordering.size(); i++) {
            const auto &column = _column_ordering[i];
            if (i == 0) {
                _logical_pipeline.push_back({OP_SCAN, column, "", ANY, MANY_TO_MANY});
            } else {
                bool found = false;
                for (size_t j = 0; j < i; j++) {
                    auto parent = _column_ordering[j];
                    for (auto &nbrs: _direction_map[parent]) {
                        if (nbrs.first == column) {
                            const auto operator_type = _is_packed ? OP_INLJ_PACKED : OP_INLJ;
                            const auto &first_col = parent;
                            const auto &second_col = column;
                            const auto relation_type = MANY_TO_MANY;
                            const auto join_direction = nbrs.second;
                            _logical_pipeline.push_back(
                                    {operator_type, first_col, second_col, join_direction, relation_type});
                            found = true;
                            break;
                        }
                    }
                }

                auto err_msg = "Cartesian product detected for: " + column;
                M_Assert(found, err_msg.c_str());
            }
        }
        if (_is_packed) {
            _logical_pipeline.push_back({OP_SINK_PACKED, "", "", ANY});
        } else {
            _logical_pipeline.push_back({OP_SINK, "", "", ANY});
        }
    }

    std::shared_ptr<Pipeline> QueryParser::build_physical_pipeline() {
        build_logical_pipeline();
        std::vector<std::shared_ptr<Operator>> physical_pipeline;
        for (auto it = _logical_pipeline.rbegin(); it != _logical_pipeline.rend(); it++) {
            const auto &ele = *it;

            switch (ele.operator_type) {
                case OP_SCAN: {
                    auto next_op = !physical_pipeline.empty() ? physical_pipeline.back() : nullptr;
                    auto scan = std::static_pointer_cast<Operator>(std::make_shared<Scan>(ele.first_col, next_op));
                    physical_pipeline.push_back(scan);
                } break;

                case OP_SINK: {
                    auto schema = create_schema();
                    auto sink = std::static_pointer_cast<Operator>(std::make_shared<Sink>(schema));
                    physical_pipeline.push_back(sink);
                } break;

                case OP_SINK_PACKED: {
                    const auto &ftree = create_factorized_tree();
                    auto sink_packed = std::static_pointer_cast<Operator>(std::make_shared<SinkPacked>(ftree));
                    physical_pipeline.push_back(sink_packed);
                } break;

                case OP_INLJ: {
                    auto next_op = !physical_pipeline.empty() ? physical_pipeline.back() : nullptr;
                    auto is_join_index_fwd = ele.join_direction == FORWARD;
                    auto extend = std::static_pointer_cast<Operator>(std::make_shared<IndexNestedLoopJoin>(
                            ele.first_col, ele.second_col, is_join_index_fwd, ele.relation_type, next_op));
                    physical_pipeline.push_back(extend);
                } break;

                case OP_INLJ_PACKED: {
                    auto next_op = !physical_pipeline.empty() ? physical_pipeline.back() : nullptr;
                    auto is_join_index_fwd = ele.join_direction == FORWARD;
                    auto extend = std::static_pointer_cast<VFEngine::Operator>(
                            std::make_shared<VFEngine::IndexNestedLoopJoinPacked>(
                                    ele.first_col, ele.second_col, is_join_index_fwd, ele.relation_type, next_op));
                    physical_pipeline.push_back(extend);
                }

                default:;
            }
        }

        auto _physical_pipeline = std::make_shared<Pipeline>(_column_names, _column_alias_map);
        _physical_pipeline->set_first_operator(physical_pipeline.back());

        return _physical_pipeline;
    }

    std::shared_ptr<FactorizedTreeElement> QueryParser::create_factorized_tree() {
        const auto factorized_tree_creator = std::make_unique<FactorizedTree>(_logical_pipeline);
        return factorized_tree_creator->build_tree();
    }

    void QueryParser::print_logical_plan() const {
        for (auto &logical_plan: _logical_pipeline) {
            std::cout << "======== \n";
            std::cout << logical_plan.first_col << " , " << logical_plan.second_col << std::endl;
            std::cout << logical_plan.operator_type << std::endl;
            std::cout << "======== \n";
        }
    }

    std::vector<LogicalPipelineElement> QueryParser::get_logical_pipeline() const { return _logical_pipeline; }

} // namespace VFEngine
