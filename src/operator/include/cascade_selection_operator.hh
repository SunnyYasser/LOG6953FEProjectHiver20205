#ifndef VFENGINE_CASCADE_SELECTION_OPERATOR_HH
#define VFENGINE_CASCADE_SELECTION_OPERATOR_HH

#include <unordered_set>
#include "../../parser/include/factorized_tree.hh"
#include "operator_definition.hh"
#ifdef MY_DEBUG
#include "../../debug/include/operator_debug.hh"
#endif

namespace VFEngine {

    /**
     * The CascadeSelection operator performs temporary upward propagation of selection
     * vectors in a factorized tree to eliminate unnecessary processing.
     *
     * For example, if a->b->c and some c values are invalid, this propagates up
     * to invalidate related b and a values that won't contribute to the final result.
     */
    class CascadeSelection : public Operator {
    public:
        CascadeSelection(const std::shared_ptr<FactorizedTreeElement> &ftree,
                         const std::shared_ptr<Operator> &next_operator);
        [[nodiscard]] operator_type_t get_operator_type() const override;
        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) override;
        [[nodiscard]] ulong get_exec_call_counter() const override;

    private:
        /**
         * Helper method to propagate selection vector invalidations upward from a node
         * @param node The node to start propagation from
         * @param backup_masks Array to store original masks for restoration
         * @param backup_nodes Array to track which nodes have been backed up
         */
        void propagate_selection_vectors_upward(const FactorizedTreeElement *node,
                                                std::unique_ptr<BitMask<State::MAX_VECTOR_SIZE>[]> &backup_masks,
                                                std::unique_ptr<const FactorizedTreeElement *[]> &backup_nodes) const;

        /**
         * Helper method to restore original selection vectors from backups
         * @param backup_masks Array containing the backed up selection masks
         * @param backup_nodes Array tracking which nodes have been backed up
         */
        void restore_selection_vectors(std::unique_ptr<BitMask<State::MAX_VECTOR_SIZE>[]> &backup_masks,
                                       std::unique_ptr<const FactorizedTreeElement *[]> &backup_nodes) const;


        /**
         * Fills the vectors in the factorized tree from the context memory
         */
        void fill_vectors_in_ftree() const;

        /**
         * Identifies and caches all leaf nodes in the factorized tree
         */
        void populate_leaves();

        /**
         * DFS helper for populate_leaves
         */
        void populate_leaves_helper(const FactorizedTreeElement *node);

        /**
         * The factorized tree for the query
         */
        std::shared_ptr<FactorizedTreeElement> _ftree;

        /**
         * Cached leaf nodes for faster access
         * Using the first letter of attribute as index (a-z)
         */
        static const FactorizedTreeElement *_leaves[26];

        /**
         * Context memory reference
         */
        std::shared_ptr<ContextMemory> _context;

        // Using unique_ptr to arrays
        std::unique_ptr<BitMask<State::MAX_VECTOR_SIZE>[]> _backup_masks;
        std::unique_ptr<const FactorizedTreeElement *[]> _backup_nodes;

        /**
         * Execution call counter
         */
        ulong _exec_call_counter = 0;

#ifdef MY_DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };

} // namespace VFEngine

#endif