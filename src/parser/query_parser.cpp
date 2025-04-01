#include "include/query_parser.hh"

#include <cascade_selection_operator.hh>
#include <cassert>
#include <factorized_tree.hh>
#include <hardcoded_linear_plan_sink_packed.hh>
#include <iostream>
#include <scan_failure_prop_operator.hh>
#include <sink_failure_prop.hh>
#include <sink_packed_min_operator.hh>
#include <sstream>
#include <unordered_set>

#include "../operator/include/index_nested_loop_join_operator.hh"
#include "../operator/include/index_nested_loop_join_packed_operator.hh"
#include "../operator/include/operator_types.hh"
#include "../operator/include/scan_operator.hh"
#include "../operator/include/sink_no_op.hh"
#include "../operator/include/sink_operator.hh"
#include "../operator/include/sink_packed_operator.hh"
#include "../operator/include/sink_packed_post_order_operator.hh"


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

    QueryParser::QueryParser(const std::string &query, const std::vector<std::string> &column_ordering,
                             const bool &is_packed, const std::vector<uint64_t> &src_nodes, const SinkType sink_type,
                             const std::vector<std::string> &column_names,
                             const std::unordered_map<std::string, std::string> &column_alias_map) :
        _query(query), _column_ordering(column_ordering), _is_packed(is_packed), _sink_type(sink_type),
        _delimiter("->"), _column_names(column_names), _column_alias_map(column_alias_map), _ftree(nullptr),
        _src_nodes(src_nodes) {
        assert(_sink_type == SinkType::FAILURE_PROP);
    }

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

#ifdef MY_DEBUG
    std::vector<std::string> get_attributes_from_joins(const std::vector<std::string> &joins,
                                                       const std::string &delimiter) {
        std::unordered_set<std::string> attributes;
        for (const auto &join: joins) {
            const size_t arrowPos = join.find(delimiter);
            if (arrowPos != std::string::npos) {
                std::string from = join.substr(0, arrowPos);
                std::string to = join.substr(arrowPos + 2);
                attributes.insert(from);
                attributes.insert(to);
            }
        }

        return {attributes.begin(), attributes.end()};
    }
#endif

    void QueryParser::build_direction_map() {
        std::vector<std::string> joins = split(_query, ',');
#ifdef MY_DEBUG
        const auto attributes = get_attributes_from_joins(joins, _delimiter);
        assert(attributes.size() == _column_ordering.size());
#endif
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
                _logical_pipeline.push_back({OP_SCAN_FAILURE_PROP, column, "", ANY, MANY_TO_MANY});
            } else {
                bool found = false;
                for (size_t j = 0; j < i; j++) {
                    auto parent = _column_ordering[j];
                    for (const auto &nbrs: _direction_map[parent]) {
                        if (nbrs.first == column) {
                            constexpr auto operator_type = OP_INLJ_PACKED;
                            const auto &first_col = parent;
                            const auto &second_col = column;
                            constexpr auto relation_type = MANY_TO_MANY;
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
        _logical_pipeline.push_back({OP_CASCADE_SELECTION, "", "", ANY});
        _logical_pipeline.push_back({OP_SINK_FAILURE_PROP, "", "", ANY});
    }

    std::shared_ptr<Pipeline> QueryParser::build_physical_pipeline() {
        build_logical_pipeline();

        // Preallocate the vector with known size N to avoid reallocations
        const size_t N = _logical_pipeline.size();
        std::vector<std::shared_ptr<Operator>> physical_pipeline;
        physical_pipeline.reserve(N);

        for (auto it = _logical_pipeline.rbegin(); it != _logical_pipeline.rend(); it++) {
            const auto &[operator_type, first_col, second_col, join_direction, relation_type] = *it;
            std::shared_ptr<Operator> next_op = !physical_pipeline.empty() ? physical_pipeline.back() : nullptr;

            switch (operator_type) {
                case OP_CASCADE_SELECTION: {
                    physical_pipeline.emplace_back(std::make_shared<CascadeSelection>(_ftree, next_op));
                } break;

                case OP_SCAN_FAILURE_PROP: {
                    physical_pipeline.emplace_back(std::make_shared<ScanFailureProp>(first_col, _src_nodes, next_op));
                } break;

                case OP_SINK_FAILURE_PROP: {
                    if (!_ftree) {
                        _ftree = create_factorized_tree();
                    }
                    physical_pipeline.emplace_back(std::make_shared<SinkFailureProp>(_ftree));
                } break;

                case OP_INLJ_PACKED: {
                    auto is_join_index_fwd = join_direction == FORWARD;
                    physical_pipeline.emplace_back(std::make_shared<VFEngine::IndexNestedLoopJoinPacked>(
                            first_col, second_col, is_join_index_fwd, relation_type, next_op));
                } break;

                default:;
            }
        }

        auto _physical_pipeline = std::make_shared<Pipeline>(_column_names, _column_alias_map);
        _physical_pipeline->set_first_operator(physical_pipeline.back());
        _physical_pipeline->set_last_operator(physical_pipeline.front());

        return _physical_pipeline;
    }

    std::shared_ptr<FactorizedTreeElement> QueryParser::create_factorized_tree() {
        const auto factorized_tree_creator = std::make_unique<FactorizedTree>(_logical_pipeline);
        return factorized_tree_creator->build_tree(_is_packed);
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
