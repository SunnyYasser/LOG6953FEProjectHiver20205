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
        const auto end = rle[parent_idx + 1];
        const auto start = rle[parent_idx];
        for (size_t i = start; i < end; i++) {
            min_values[attr_idx] = std::min(min_values[attr_idx], data[i]);
        }
    }

    // For internal nodes (general case)
    static void update_min_internal(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx,
                                    ulong *min_values) {
        const auto &vec = op->_value;
        const auto &children = op->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &size = vec->_state->_state_info._size;
        const auto &rle = vec->_state->_rle;
        const auto attr_idx = op->_attribute[0] - 'a'; // Convert attribute name to index
        const auto &values = vec->_values;

        const auto start = std::max(rle[parent_idx], static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);

        for (auto i = start; i < end; i++) {
            min_values[attr_idx] = std::min(min_values[attr_idx], values[i]);
            for (const auto &node: children) {
                if (node->_children.empty()) {
                    update_min_leaf(node, i, min_values);
                } else {
                    update_min_internal(node, i, min_values);
                }
            }
        }
    }

    static void update_min_values(const std::shared_ptr<FactorizedTreeElement> &root, ulong *min_values) {
        const auto &vec = root->_value;
        const auto &children = root->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &curr_size = vec->_state->_state_info._size;
        const auto attr_idx = root->_attribute[0] - 'a'; // Convert attribute name to index
        const auto &values = vec->_values;

        for (auto i = 0; i < curr_size; i++) {
            const auto parent_idx = static_cast<uint32_t>(start_pos + i);
            min_values[attr_idx] = std::min(min_values[attr_idx], values[parent_idx]);
            for (const auto &node: children) {
                if (node->_children.empty()) {
                    update_min_leaf(node, parent_idx, min_values);
                } else {
                    update_min_internal(node, parent_idx, min_values);
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
