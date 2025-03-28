#include "include/cascade_selection_operator.hh"
#include <limits>
#include <memory>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    const FactorizedTreeElement *CascadeSelection::_leaves[26] = {
            nullptr}; // This will zero-initialize all elements to nullptr

    CascadeSelection::CascadeSelection(const std::shared_ptr<FactorizedTreeElement> &ftree,
                                       const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _ftree(ftree), _backup_masks(nullptr), _backup_nodes(nullptr) {

#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t CascadeSelection::get_operator_type() const { return OP_CASCADE_SELECTION; }

    void CascadeSelection::propagate_selection_vectors_upward(
            const FactorizedTreeElement *node, std::unique_ptr<BitMask<State::MAX_VECTOR_SIZE>[]> &backup_masks,
            std::unique_ptr<const FactorizedTreeElement *[]> &backup_nodes) const {

        if (!node) {
            // If null, stop recursion
            return;
        }

        // Process the current node's children to update its selection mask
        const auto &node_vec = node->_value;
        auto &node_selection_mask = *(node_vec->_state->_selection_mask);

        // Backup node's selection mask if is not already backed up
        const char node_char = node->_attribute[0];
        const int node_idx = node_char - 'a';

        if (!backup_nodes[node_idx]) {
            // Create backup if not already done
            COPY_BITMASK(State::MAX_VECTOR_SIZE, backup_masks[node_idx], node_selection_mask);
            backup_nodes[node_idx] = node;
        }

        // Get valid ranges for node
        const int32_t node_start = GET_START_POS(node_selection_mask);
        const int32_t node_end = GET_END_POS(node_selection_mask);

        // Process each of the node's children
        bool is_modified = false;
        for (const auto &child_ptr: node->_children) {
            const auto &child_vec = child_ptr->_value;
            const BitMask<State::MAX_VECTOR_SIZE> &child_selection_mask = *(child_vec->_state->_selection_mask);

            // Get RLE information for node-child relationship
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
            const uint32_t *rle = child_vec->_state->_rle;
#else
            const uint32_t *rle = child_vec->_state->_rle.get();
#endif

            const auto child_start_pos = GET_START_POS(child_selection_mask);
            const auto child_end_pos = GET_END_POS(child_selection_mask);

            // For each valid node value, check if all its children are invalid
            for (int32_t parent_idx = node_start; parent_idx <= node_end; parent_idx++) {
                // Skip if parent already invalid
                if (!TEST_BIT(node_selection_mask, parent_idx)) {
                    continue;
                }

                // Get child range for this parent using RLE
                const int32_t child_start = rle[parent_idx];
                const int32_t child_end = rle[parent_idx + 1] - 1;

                // Check if parent has any children at all
                if (child_end < child_start) {
                    // No children, mark parent as invalid
                    CLEAR_BIT(node_selection_mask, parent_idx);
                    is_modified = true;
                    continue;
                }

                const auto actual_start_idx = std::max(child_start, child_start_pos);
                const auto actual_end_idx = std::min(child_end, child_end_pos);

                // Check if all children for this parent are invalid
                bool all_children_invalid = true;
                for (auto child_idx = actual_start_idx; child_idx <= actual_end_idx; child_idx++) {
                    if (TEST_BIT(child_selection_mask, child_idx)) {
                        // Found a valid child, parent should remain valid
                        all_children_invalid = false;
                        break;
                    }
                }

                // If all children are invalid, mark parent as invalid
                if (all_children_invalid) {
                    CLEAR_BIT(node_selection_mask, parent_idx);
                    is_modified = true;
                }
            }
        }

        // Only update ranges and propagate if the selection mask was modified
        if (is_modified) {
            // Update start/end positions in node selection mask
            const int32_t curr_node_start = GET_START_POS(node_selection_mask);
            const int32_t curr_node_end = GET_END_POS(node_selection_mask);

            // Find first valid bit
            int32_t new_start = State::MAX_VECTOR_SIZE;
            for (int32_t i = curr_node_start; i <= curr_node_end; i++) {
                if (TEST_BIT(node_selection_mask, i)) {
                    new_start = i;
                    break;
                }
            }

            // Find last valid bit
            int32_t new_end = 0;
            for (int32_t i = curr_node_end; i >= curr_node_start; i--) {
                if (TEST_BIT(node_selection_mask, i)) {
                    new_end = i;
                    break;
                }
            }

            // Update node selection mask positions if there are any valid bits
            if (new_start <= new_end) {
                SET_START_POS(node_selection_mask, new_start);
                SET_END_POS(node_selection_mask, new_end);
            } else {
                // No valid bits - set invalid range
                SET_START_POS(node_selection_mask, State::MAX_VECTOR_SIZE - 1);
                SET_END_POS(node_selection_mask, 0);
            }

            // Recursively propagate up to the root (if this node has a parent)
            propagate_selection_vectors_upward(node->_parent, backup_masks, backup_nodes);
        } else {
            // Only clear backup_nodes references (which are read-only)
            // but keep backup_masks (which contain the actual data we're modifying)
            backup_nodes[node_idx] = nullptr;
        }
    }

    void
    CascadeSelection::restore_selection_vectors(std::unique_ptr<BitMask<State::MAX_VECTOR_SIZE>[]> &backup_masks,
                                                std::unique_ptr<const FactorizedTreeElement *[]> &backup_nodes) const {

        // Restore all backed up selection masks
        for (int i = 0; i < 26; i++) {
            if (backup_nodes[i]) {
                const auto node = backup_nodes[i];

                // Restore the original selection mask
                auto &node_vec = node->_value;
                auto &node_selection_mask = *(node_vec->_state->_selection_mask);
                COPY_BITMASK(State::MAX_VECTOR_SIZE, node_selection_mask, backup_masks[i]);

                // Only clear backup_nodes references
                // Keep backup_masks allocated for potential reuse
                backup_nodes[i] = nullptr;
            }
        }
    }

    void CascadeSelection::execute() {
        _exec_call_counter++;

#ifdef MY_DEBUG
        _debug->log_operator_debug_msg();
#endif

        // Clear backup tracking
        for (int i = 0; i < 26; i++) {
            _backup_nodes[i] = nullptr;
        }

        // Process parent nodes of leaf nodes
        for (int i = 0; i < 26; i++) {
            if (_leaves[i]) {
                const auto &leaf = _leaves[i];

                // We only need to process if the leaf has a parent
                if (leaf->_parent) {
                    // Start propagation from the parent of the leaf
                    propagate_selection_vectors_upward(leaf->_parent, _backup_masks, _backup_nodes);
                }
            }
        }

        get_next_operator()->execute();

        // Restore original selection vectors
        restore_selection_vectors(_backup_masks, _backup_nodes);
    }

    static void dfs_helper(const std::shared_ptr<FactorizedTreeElement> &root, std::unordered_set<std::string> &visited,
                           const std::shared_ptr<ContextMemory> &context) {
        const auto &attr = root->_attribute;
        if (visited.find(attr) != visited.end())
            return;

        root->_value = context->read_vector_for_column(attr);
        visited.insert(attr);

        const auto &children = root->_children;
        for (const auto &child: children) {
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
            // This is a leaf node - store it using attribute name (a-z) as index
            // Assuming all attributes are lowercase single alphabets
            const char attr_char = node->_attribute[0];
            const int idx = attr_char - 'a';

            // Validate index is in range (0-25)
            if (idx >= 0 && idx < 26) {
                _leaves[idx] = node;
                return;
            }
        }

        for (const auto &child: node->_children) {
            populate_leaves_helper(child.get());
        }
    }

    void CascadeSelection::init(const std::shared_ptr<ContextMemory> &context,
                                const std::shared_ptr<DataStore> &datastore) {
        _context = context;

        // Allocate memory for backup arrays
        _backup_masks = std::make_unique<BitMask<State::MAX_VECTOR_SIZE>[]>(26);
        _backup_nodes = std::make_unique<const FactorizedTreeElement *[]>(26);

        // Initialize arrays
        for (int i = 0; i < 26; i++) {
            _backup_nodes[i] = nullptr;
        }

        // Fill vectors in the factorized tree
        fill_vectors_in_ftree();

        // Identify and cache leaf nodes
        populate_leaves();

        // Initialize next operator if any
        if (get_next_operator()) {
            get_next_operator()->init(context, datastore);
        }
    }

    ulong CascadeSelection::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine