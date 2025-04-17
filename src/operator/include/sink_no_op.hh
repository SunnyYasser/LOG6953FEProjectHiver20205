#ifndef VFENGINE_SINK_NO_OP_OPERATOR_HH
#define VFENGINE_SINK_NO_OP_OPERATOR_HH


#include "operator_definition.hh"
#include "operator_types.hh"
#include "schema.hh"
#ifdef MY_DEBUG
#include "../../debug/include/operator_debug.hh"
#endif


namespace VFEngine {
    class SinkNoOp final : public Operator {
    public:
        SinkNoOp();
        SinkNoOp(const SinkNoOp &) = delete;
        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        [[nodiscard]] unsigned long get_exec_call_counter() const override;
        static ulong get_total_row_size_if_materialized();

    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;
        unsigned long _exec_call_counter{};
#ifdef MY_DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif
