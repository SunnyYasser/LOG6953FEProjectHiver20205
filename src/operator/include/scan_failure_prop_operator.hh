#ifndef SCAN_FAILURE_PROP_OPERATOR_HH
#define SCAN_FAILURE_PROP_OPERATOR_HH
#include "operator_definition.hh"
#include "operator_types.hh"

namespace VFEngine
{
    class ScanFailureProp final : public Operator {

    public:
        ScanFailureProp() = delete;
        ScanFailureProp(const std::string &scan_attribute, const std::vector<uint64_t>& src_nodes, const std::shared_ptr<Operator> &next_operator);

        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        [[nodiscard]] unsigned long get_exec_call_counter() const override;


    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;
        Vector *_output_vector;
        BitMask<State::MAX_VECTOR_SIZE> **_output_selection_mask;
        const std::string _attribute;
        const std::vector<uint64_t> _src_nodes;
        unsigned long _exec_call_counter{};
    };
}; // namespace VFEngine

#endif