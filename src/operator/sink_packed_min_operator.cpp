#include "include/sink_packed_min_operator.hh"
#include <limits>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    // Initialize static member
    ulong SinkPackedMin::_min_values_attribute[26] = {
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max(),
            std::numeric_limits<ulong>::max(), std::numeric_limits<ulong>::max()};

    SinkPackedMin::SinkPackedMin(const std::shared_ptr<FactorizedTreeElement> &ftree) : Operator(), _ftree(ftree) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t SinkPackedMin::get_operator_type() const { return OP_SINK_PACKED_MIN; }

    const ulong *SinkPackedMin::get_min_values() { return _min_values_attribute; }

    // For leaf nodes
    static void update_min_leaf(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx,
                                ulong *min_values) {
        const auto &vec = op->_value;
        const auto &data = vec->_values;
        const auto attr_idx = op->_attribute[0] - 'a'; // Convert attribute name to index
        const auto &rle = vec->_state->_rle;
        const auto &selection_mask = *(vec->_state->_selection_mask);

        // Check if parent_idx is valid
        if (!TEST_BIT(selection_mask, parent_idx)) {
            return;
        }

        // Use RLE to determine the range
        uint32_t start, end;
        if (parent_idx == 0) {
            start = 0;
            end = rle[parent_idx + 1];
        } else {
            start = rle[parent_idx - 1];
            end = rle[parent_idx];
        }

        // Update min values for this attribute within the range
        for (size_t i = start; i < end; i++) {
            min_values[attr_idx] = std::min(min_values[attr_idx], data[i]);
        }
    }

    // For internal nodes (general case)
    static void update_min_internal(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx,
                                    ulong *min_values) {
        const auto &vec = op->_value;
        const auto &children = op->_children;
        const auto &values = vec->_values;
        const auto &selection_mask = *(vec->_state->_selection_mask);
        const auto attr_idx = op->_attribute[0] - 'a'; // Convert attribute name to index

        // Get the valid range from the selection mask
        const auto start_pos = GET_START_POS(selection_mask);
        const auto end_pos = GET_END_POS(selection_mask);

        // Check if there's any valid range
        if (start_pos > end_pos) {
            return;
        }

        // Check if parent_idx is valid and in range
        if (parent_idx < start_pos || parent_idx > end_pos || !TEST_BIT(selection_mask, parent_idx)) {
            return;
        }

        // Update min value for this attribute
        min_values[attr_idx] = std::min(min_values[attr_idx], values[parent_idx]);

        // Process all children
        for (const auto &node: children) {
            if (node->_children.empty()) {
                update_min_leaf(node, parent_idx, min_values);
            } else {
                update_min_internal(node, parent_idx, min_values);
            }
        }
    }

    static void update_min_values(const std::shared_ptr<FactorizedTreeElement> &root, ulong *min_values) {
        const auto &vec = root->_value;
        const auto &children = root->_children;
        const auto &values = vec->_values;
        const auto &selection_mask = *(vec->_state->_selection_mask);
        const auto attr_idx = root->_attribute[0] - 'a'; // Convert attribute name to index

        // Get the valid range from the selection mask
        const auto start_pos = GET_START_POS(selection_mask);
        const auto end_pos = GET_END_POS(selection_mask);

        // Check if there's any valid range
        if (start_pos > end_pos) {
            return;
        }

        // Process all valid indices in the range
        for (auto idx = start_pos; idx <= end_pos; idx++) {
            // Skip invalid indices
            if (!TEST_BIT(selection_mask, idx)) {
                continue;
            }

            // Update min value for this attribute
            min_values[attr_idx] = std::min(min_values[attr_idx], values[idx]);

            // Process all children
            for (const auto &node: children) {
                if (node->_children.empty()) {
                    update_min_leaf(node, idx, min_values);
                } else {
                    update_min_internal(node, idx, min_values);
                }
            }
        }
    }

    void SinkPackedMin::execute() {
        _exec_call_counter++;
        update_min_values(_ftree, _min_values_attribute);
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

    void SinkPackedMin::fill_vectors_in_ftree() const {
        std::unordered_set<std::string> visited;
        dfs_helper(_ftree, visited, _context);
    }

    void SinkPackedMin::init(const std::shared_ptr<ContextMemory> &context,
                             const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
    }

    ulong SinkPackedMin::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
