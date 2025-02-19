#include "include/cascade_selection_operator.hh"
#include <limits>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    // Initialize static member
    const FactorizedTreeElement *CascadeSelection::_leaves[26] = {
            nullptr}; // This will zero-initialize all elements to nullptr

    CascadeSelection::CascadeSelection(const std::shared_ptr<FactorizedTreeElement> &ftree) : Operator(), _ftree(ftree) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t CascadeSelection::get_operator_type() const { return OP_CASCADE_SELECTION; }

    void CascadeSelection::selection_vector_upwards_propagation_helper(const FactorizedTreeElement *node) const {
        if (!node or !node->_parent) {
            // if root reached or null
            return;
        }
        // Get current node's selection mask
        const auto &vec = node->_value;
        const BitMask<State::MAX_VECTOR_SIZE> &leaf_selection_mask = *(vec->_state->_selection_mask);

        // Get parent's selection mask
        const auto parent = node->_parent;
        const auto &parent_vec = parent->_value;
        auto &parent_selection_mask = *(parent_vec->_state->_selection_mask);

        // Update parent's selection mask with AND of both masks
        AND_BITMASKS(State::MAX_VECTOR_SIZE, parent_selection_mask, leaf_selection_mask);

        // Recursively propagate up to the root
        selection_vector_upwards_propagation_helper(parent);
    }

    void CascadeSelection::selection_vector_upwards_propagation() const {
        for (size_t i = 0; i < 26; i++) {
            if (_leaves[i]) {
                selection_vector_upwards_propagation_helper(_leaves[i]);
            }
        }
    }

    void CascadeSelection::execute() {
        _exec_call_counter++;
        selection_vector_upwards_propagation();
    }

    static void dfs_helper(const std::shared_ptr<FactorizedTreeElement> &root, std::unordered_set<std::string> &visited,
                           const std::shared_ptr<ContextMemory> &context) {
        const auto attr = root->_attribute;
        if (visited.find(attr) != visited.end()) {
            return;
        }

        const auto vec = context->read_vector_for_column(attr);
        root->_value = vec;
        visited.insert(attr);

        if (root->_children.empty()) {
            return;
        }

        for (const auto &child: root->_children) {
            dfs_helper(child, visited, context);
        }
    }

    void CascadeSelection::fill_vectors_in_ftree() const {
        std::unordered_set<std::string> visited;
        dfs_helper(_ftree, visited, _context);
    }

    void CascadeSelection::populate_leaves() { populate_leaves_helper(_ftree.get()); }

    void CascadeSelection::populate_leaves_helper(const FactorizedTreeElement *node) {
        if (!node)
            return;

        if (node->_children.empty()) {
            // This is a leaf node
            int idx = node->_attribute[0] - 'a';
            _leaves[idx] = node;
            return;
        }

        for (const auto &child: node->_children) {
            populate_leaves_helper(child.get());
        }
    }


    void CascadeSelection::init(const std::shared_ptr<ContextMemory> &context,
                             const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
        populate_leaves();
    }

    ulong CascadeSelection::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
