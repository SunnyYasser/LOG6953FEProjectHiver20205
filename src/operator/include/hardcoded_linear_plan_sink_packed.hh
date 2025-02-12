#ifndef VFENGINE_SINK_PACKED_HARDCODED_LINEAR_PLAN_HH
#define VFENGINE_SINK_PACKED_HARDCODED_LINEAR_PLAN_HH

#include "../../parser/include/factorized_tree.hh"
#include "operator_definition.hh"
#include "operator_types.hh"

#ifdef MY_DEBUG
#include "../../debug/include/operator_debug.hh"
#endif

namespace VFEngine {
    class SinkLinearHardcoded final : public Operator {
    public:
        SinkLinearHardcoded() = delete;

        SinkLinearHardcoded(const SinkLinearHardcoded &) = delete;

        explicit SinkLinearHardcoded(const std::shared_ptr<FactorizedTreeElement> &ftree);

        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        static ulong get_total_row_size_if_materialized();
        [[nodiscard]] unsigned long get_exec_call_counter() const override;


    private:
        void update_total_row_size_if_materialized();
        void update_total_column_size_if_materialized();
        void fill_vectors_in_ftree() const;
        void capture_leaf_node(const std::shared_ptr<FactorizedTreeElement> &root);
        [[nodiscard]] operator_type_t get_operator_type() const override;
        std::shared_ptr<FactorizedTreeElement> _ftree;
        static unsigned long total_row_size_if_materialized;
        unsigned long _exec_call_counter{};
        std::shared_ptr<ContextMemory> _context;
        const std::shared_ptr<State>* _leaf_state;

#ifdef MY_DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif //VFENGINE_SINK_PACKED_HARDCODED_LINEAR_PLAN_HH
