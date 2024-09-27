#ifndef VFENGINE_INDEX_NESTED_LOOP_JOIN_1_TO_1_HH
#define VFENGINE_INDEX_NESTED_LOOP_JOIN_1_TO_1_HH

#include <relation_types.hh>
#include "operator_definition.hh"
#include "operator_types.hh"

/*
 * Idea is we know that entire table we are working on is 1 to 1 relation, ie,
 * each element has a fwd and bwd adjlist of size 1
 * we override the execute() such that we do not change input vector's pos
 * from -1 while fillng the output vector, and the states are shared between
 * the two vectors
 */

namespace VFEngine {
    class IndexNestedLoopJointo1 final : public Operator {
    public:
        IndexNestedLoopJointo1() = delete;

        IndexNestedLoopJointo1(const IndexNestedLoopJointo1 &) = delete;

        IndexNestedLoopJointo1(const std::string &input_attribute, const std::string &output_attribute,
                               const bool &is_join_index_fwd, const RelationType &relation_type,
                               const std::shared_ptr<Operator> &next_operator);

        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        ulong get_exec_call_counter() const;


    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;
        void execute_internal(const std::string &fn_name);

        Vector *_input_vector;
        Vector *_output_vector;
        bool _is_join_index_fwd;
        const RelationType _relation_type;
        const std::string _input_attribute, _output_attribute;
        const std::unique_ptr<AdjList[]> *_adj_list{};
        ulong _exec_call_counter{};
#ifdef DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif
