#ifndef VFENGINE_SCAN_OPERATOR_HH
#define VFENGINE_SCAN_OPERATOR_HH

#include "operator_definition.hh"
#include "operator_types.hh"

namespace VFEngine

{
    class Scan : public Operator {

    public:
        Scan() = delete;
        Scan(const Scan &) = delete;
        Scan(const std::vector<std::string> &) = delete;
        Scan(const std::string &table_name, const std::string &scan_attribute,
             const std::shared_ptr<Operator> &next_operator);


        void execute() override;
        void debug() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;

    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;
        uint64_t _max_id_value;
        const std::string _attribute;
        std::shared_ptr<ContextMemory> _context_memory;
    };
}; // namespace VFEngine

#endif
