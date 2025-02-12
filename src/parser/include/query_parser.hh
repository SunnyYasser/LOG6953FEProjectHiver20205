#ifndef VFENGINE_QUERY_PARSER_HH
#define VFENGINE_QUERY_PARSER_HH

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "../../engine/include/pipeline.hh"
#include "factorized_tree.hh"
#include "logical_pipeline_element.hh"

#ifdef MY_DEBUG
#include "../../debug/include/parser_debug.hh"
#endif

namespace VFEngine {
    enum class SinkType { UNPACKED = 0, PACKED, NO_OP, PACKED_VECTORIZED, HARDCODED_LINEAR, PACKED_MIN};

    class QueryParser {
    public:
        QueryParser(const std::string &query, const std::vector<std::string> &column_ordering, const bool &is_packed,
                    SinkType sink_type, const std::vector<std::string> &column_names,
                    const std::unordered_map<std::string, std::string> &column_alias_map);

        QueryParser(const std::string &query, const std::vector<std::string> &column_ordering, const bool &is_packed,
                    SinkType sink_type, const std::vector<std::string> &column_names,
                    const std::unordered_map<std::string, std::string> &column_alias_map,
                    const std::shared_ptr<FactorizedTreeElement> &ftree);

        std::shared_ptr<Pipeline> build_physical_pipeline();
        std::shared_ptr<FactorizedTreeElement> create_factorized_tree();
        void print_logical_plan() const;
        std::vector<LogicalPipelineElement> get_logical_pipeline() const;
        ~QueryParser() = default;

    private:
        std::string _query;
        std::vector<std::string> _column_ordering;
        SinkType _sink_type;
        bool _is_packed;
        std::string _delimiter;
        std::vector<std::string> _column_names;
        std::unordered_map<std::string, std::string> _column_alias_map;
        std::vector<LogicalPipelineElement> _logical_pipeline;
        std::unordered_map<std::string, std::vector<std::pair<std::string, JoinDirection>>> _direction_map;
        std::shared_ptr<FactorizedTreeElement> _ftree;
        void build_direction_map();
        void build_logical_pipeline();
        std::vector<std::pair<std::string, SchemaType>> create_schema();

#ifdef MY_DEBUG
        std::unique_ptr<ParserDebugUtility> _debug;
#endif
    };
} // namespace VFEngine

#endif
