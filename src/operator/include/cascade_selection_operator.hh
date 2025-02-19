#ifndef VFENGINE_CASCADE_SELECTION_OPERATOR_HH
#define VFENGINE_CASCADE_SELECTION_OPERATOR_HH

/*
* Propogate the selection vector upwards from leaves to root
*/

#include "../../parser/include/factorized_tree.hh"
#include "operator_definition.hh"
#include "operator_types.hh"

#ifdef MY_DEBUG
#include "../../debug/include/operator_debug.hh"
#endif

namespace VFEngine {
    class CascadeSelection final : public Operator {
    public:
        CascadeSelection() = delete;

        CascadeSelection(const CascadeSelection &) = delete;

        explicit CascadeSelection(const std::shared_ptr<FactorizedTreeElement> &ftree);

        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        [[nodiscard]] unsigned long get_exec_call_counter() const override;


    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;
        void populate_leaves();
        void fill_vectors_in_ftree() const;
        void populate_leaves_helper(const FactorizedTreeElement *node);
        void selection_vector_upwards_propagation_helper(const FactorizedTreeElement *node) const;
        void selection_vector_upwards_propagation() const;
        static const FactorizedTreeElement *_leaves[26];
        std::shared_ptr<FactorizedTreeElement> _ftree;
        unsigned long _exec_call_counter{};
        std::shared_ptr<ContextMemory> _context;
#ifdef MY_DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif
