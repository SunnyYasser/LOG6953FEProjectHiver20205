#ifndef VFENGINE_SINK_PACKED_MIN_OPERATOR_HH
#define VFENGINE_SINK_PACKED_MIN_OPERATOR_HH

#include "../../parser/include/factorized_tree.hh"
#include "operator_definition.hh"
#include "operator_types.hh"

#ifdef MY_DEBUG
#include "../../debug/include/operator_debug.hh"
#endif

namespace VFEngine {
    class SinkPackedMin final : public Operator {
    public:
        SinkPackedMin() = delete;
        SinkPackedMin(const SinkPackedMin &) = delete;
        explicit SinkPackedMin(const std::shared_ptr<FactorizedTreeElement> &ftree);
        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        [[nodiscard]] unsigned long get_exec_call_counter() const override;
        static const ulong *get_min_values();

    private:
        void fill_vectors_in_ftree() const;
        static ulong _min_values_attribute[26]; // assuming all attributes have a single character in range 'a' to 'z'
        [[nodiscard]] operator_type_t get_operator_type() const override;
        std::shared_ptr<FactorizedTreeElement> _ftree;
        unsigned long _exec_call_counter{};
        std::shared_ptr<ContextMemory> _context;
#ifdef MY_DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif
