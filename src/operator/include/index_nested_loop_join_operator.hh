//
// Created by Sunny on 16-06-2024.
//

#ifndef VFENGINE_INDEX_NESTED_LOOP_JOIN_OPERATOR_HH
#define VFENGINE_INDEX_NESTED_LOOP_JOIN_OPERATOR_HH

#include "operator_definition.hh"
#include "operator_types.hh"

#include <relation_types.hh>

namespace VFEngine {
    class IndexNestedLoopJoin final : public Operator {
    public:
        IndexNestedLoopJoin() = delete;

        IndexNestedLoopJoin(const IndexNestedLoopJoin &) = delete;

        IndexNestedLoopJoin(const std::string &input_attribute, const std::string &output_attribute,
                            const bool &is_join_index_fwd, const RelationType &relation_type,
                            const std::shared_ptr<Operator> &next_operator);

        void execute() override;
        void debug() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        ulong get_exec_call_counter() const;


    private:
        void execute_in_chunks_non_incremental();
        void execute_in_chunks_incremental();
        [[nodiscard]] operator_type_t get_operator_type() const override;
        bool should_enable_state_sharing() const;
        void execute_internal(const std::string &fn_name, const std::string &operator_name);

        Vector *_input_vector;
        Vector *_output_vector;
        bool _is_join_index_fwd;
        const RelationType _relation_type;
        const std::string _input_attribute, _output_attribute;
        const std::unique_ptr<AdjList[]>* _adj_list{};
        uint64_t _adj_list_size{};
        ulong _exec_call_counter{};
    };
}; // namespace VFEngine

#endif
