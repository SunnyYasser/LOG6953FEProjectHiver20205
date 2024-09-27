#ifndef VFENGINE_SCAN_OPERATOR_HH
#define VFENGINE_SCAN_OPERATOR_HH

#include "operator_definition.hh"
#include "operator_types.hh"

namespace VFEngine

{
    class Scan final : public Operator {

    public:
        Scan() = delete;
        Scan(const Scan &) = delete;
        Scan(const std::vector<std::string> &) = delete;
        Scan(const std::string &scan_attribute, const std::shared_ptr<Operator> &next_operator);

        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        [[nodiscard]] ulong get_exec_call_counter() const;


    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;
        Vector *_output_vector;
        uint64_t _max_id_value;
        const std::string _attribute;
        ulong _exec_call_counter{};
#ifdef DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif
