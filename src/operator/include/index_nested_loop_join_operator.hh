//
// Created by Sunny on 16-06-2024.
//

#ifndef VFENGINE_INDEX_NESTED_LOOP_JOIN_OPERATOR_HH
#define VFENGINE_INDEX_NESTED_LOOP_JOIN_OPERATOR_HH

#include "../../memory/include/vector.hh"
#include "operator_definition.hh"
#include "operator_types.hh"

#include <relation_types.hh>

namespace VFEngine {
    class IndexNestedLoopJoin : public Operator {
    public:
        IndexNestedLoopJoin() = delete;

        IndexNestedLoopJoin(const IndexNestedLoopJoin &) = delete;

        IndexNestedLoopJoin(const std::string &table, const std::string &input_attribute,
                            const std::string &output_attribute, const bool &is_join_index_fwd,
                            const RelationType &relation_type, const std::shared_ptr<Operator> &next_operator);

        void execute() override;

        void debug() override;

        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;

    private:
        void execute_in_chunks_non_incremental();
        void execute_in_chunks_incremental();
        [[nodiscard]] operator_type_t get_operator_type() const override;
        Vector create_new_state(const Vector &);
        bool should_enable_state_sharing() const;
        void execute_internal(const std::string &fn_name, const std::string &operator_name);

    private:
        bool _is_join_index_fwd;
        RelationType _relation_type;
        const std::string _input_attribute, _output_attribute;
        std::unordered_map<int32_t, std::vector<int32_t>> _adj_list;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _datastore;
    };
}; // namespace VFEngine

#endif
