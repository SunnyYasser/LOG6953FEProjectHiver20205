#ifndef SINK_FAILURE_PROP_HH
#define SINK_FAILURE_PROP_HH

#include "../../parser/include/factorized_tree.hh"
#include "operator_definition.hh"
#include "operator_types.hh"

namespace VFEngine {
    class SinkFailureProp final : public Operator {
    public:
        SinkFailureProp() = delete;
        SinkFailureProp(const SinkFailureProp &) = delete;
        explicit SinkFailureProp(const std::shared_ptr<FactorizedTreeElement> &ftree);
        void execute() override;
        void count_leaf(const std::shared_ptr<FactorizedTreeElement> &op, uint32_t parent_idx);
        void count_internal(const std::shared_ptr<FactorizedTreeElement> &op, uint32_t parent_idx);
        void count(const std::shared_ptr<FactorizedTreeElement> &root);
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        [[nodiscard]] std::vector<uint64_t>* get_total_rows() const;
        [[nodiscard]] unsigned long get_exec_call_counter() const override;

    private:
        void update_total_rows();
        void fill_vectors_in_ftree() const;
        [[nodiscard]] operator_type_t get_operator_type() const override;
        std::shared_ptr<FactorizedTreeElement> _ftree;
        std::shared_ptr<std::vector<uint64_t>> _total_rows;
        unsigned long _exec_call_counter{};
        std::shared_ptr<ContextMemory> _context;
    };
}; // namespace VFEngine

#endif
