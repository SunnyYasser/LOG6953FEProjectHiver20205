#ifndef SAMPLE_DB_SCAN_OPERATOR_HH
#define SAMPLE_DB_SCAN_OPERATOR_HH

#include "operator_definition.hh"
#include "operator_types.hh"

namespace SampleDB

{
    class Scan : public Operator {

    public:
        Scan() = delete;
        Scan(const Scan &) = delete;
        Scan(const std::vector<std::string> &) = delete;
        Scan(const std::string &, const std::string &, const std::shared_ptr<Schema> &,
             const std::shared_ptr<Operator> &);

    public:
        void execute() override;
        void debug() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;

    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;
        const std::string _attribute;
        std::vector<int32_t> _attribute_data;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _data_store;
    };
}; // namespace SampleDB

#endif
