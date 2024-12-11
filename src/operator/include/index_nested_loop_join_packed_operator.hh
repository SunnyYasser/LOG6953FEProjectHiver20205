#ifndef VFENGINE_INDEX_NESTED_LOOP_JOIN_PACKED_OPERATOR_HH
#define VFENGINE_INDEX_NESTED_LOOP_JOIN_PACKED_OPERATOR_HH

#include "operator_definition.hh"
#include "operator_types.hh"
#include "relation_types.hh"
#ifdef MY_DEBUG
#include "../../debug/include/operator_debug.hh"
#endif


namespace VFEngine {
    class IndexNestedLoopJoinPacked final : public Operator {
    public:
        IndexNestedLoopJoinPacked() = delete;

        IndexNestedLoopJoinPacked(const IndexNestedLoopJoinPacked &) = delete;

        IndexNestedLoopJoinPacked(const std::string &input_attribute, const std::string &output_attribute,
                                  const bool &is_join_index_fwd, const RelationType &relation_type,
                                  const std::shared_ptr<Operator> &next_operator);

        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        [[nodiscard]] unsigned long get_exec_call_counter() const override;


    private:
        void execute_internal();
        [[nodiscard]] operator_type_t get_operator_type() const override;


        Vector *_input_vector;
        Vector *_output_vector;
        bool _is_join_index_fwd;
        const RelationType _relation_type;
        const std::string _input_attribute, _output_attribute;
        const std::unique_ptr<AdjList[]> *_adj_list{};
        uint64_t _adj_list_size{};
        unsigned long _exec_call_counter{};

#ifdef MY_DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif
