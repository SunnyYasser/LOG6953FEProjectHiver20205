#include "include/sink_packed_operator.hh"

#include <iostream>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    ulong SinkPacked::total_row_size_if_materialized = 0;

    SinkPacked::SinkPacked(const std::shared_ptr<FactorizedTreeElement> &ftree) : Operator(), _ftree(ftree) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t SinkPacked::get_operator_type() const { return OP_SINK_PACKED; }

    void SinkPacked::execute() {
        _exec_call_counter++;
        update_total_row_size_if_materialized();
    }

#ifdef ENABLE_BRANCHLESS_SINK_PACKED
    ulong count_leaf(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
        const auto &vec = op->_value;
        const auto &rle = vec->_state->_rle;
        const auto &children = op->_children;
        std::cout << "EBP_LEAF" << std::endl;
#ifdef REMOVE_MEMSET
        std::cout << "EBP_RM_LEAF" << std::endl;
        const auto &rle_start_pos = vec->_state->_rle_start_pos;
        return (parent_idx + 1 >= rle_start_pos) *
               (rle[parent_idx + 1] - rle[parent_idx] * (parent_idx + 1 > rle_start_pos));
#else
        return rle[parent_idx + 1] - rle[parent_idx];
#endif
    }

#else
    // For leaf nodes
    ulong count_leaf(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
        const auto &vec = op->_value;
        const auto &rle = vec->_state->_rle;
        const auto &children = op->_children;
        std::cout << "NO_EBP_LEAF" << std::endl;
#ifdef REMOVE_MEMSET
        std::cout << "NO_EBP_RM_LEAF" << std::endl;
        const auto &rle_start_pos = vec->_state->_rle_start_pos;
        if (parent_idx + 1 < rle_start_pos) {
            return 0;
        }
        return parent_idx + 1 > rle_start_pos ? rle[parent_idx + 1] - rle[parent_idx] : rle[parent_idx + 1];
#else
        return rle[parent_idx + 1] - rle[parent_idx];
#endif
    }

#endif

#ifdef ENABLE_BRANCHLESS_SINK_PACKED
    ulong count_internal(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
        const auto &vec = op->_value;
        const auto &children = op->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &size = vec->_state->_state_info._size;
        const auto &rle = vec->_state->_rle;

#ifdef REMOVE_MEMSET
        const auto &rle_start_pos = vec->_state->_rle_start_pos;
        auto rle_val = parent_idx == rle_start_pos - 1 ? 0 : rle[parent_idx];
        const auto start = std::max(rle_val, static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);
#else
        const auto start = std::max(rle[parent_idx], static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);
#endif

        ulong sum = 0;
        for (auto i = start; i < end; i++) {
            ulong value = 1;
            for (const auto &node: children) {
                value *= (node->_children.empty() ? count_leaf(node, i) : count_internal(node, i));
            }
            sum += value;
        }
        return sum;
    }

#else

    // For internal nodes (general case)
    ulong count_internal(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
        const auto &vec = op->_value;
        const auto &children = op->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &size = vec->_state->_state_info._size;
        const auto &rle = vec->_state->_rle;

#ifdef REMOVE_MEMSET
        const auto &rle_start_pos = vec->_state->_rle_start_pos;
        auto rle_val = parent_idx == rle_start_pos - 1 ? 0 : rle[parent_idx];
        const auto start = std::max(rle_val, static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);
#else
        const auto start = std::max(rle[parent_idx], static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);
#endif

        ulong sum = 0;
        for (auto i = start; i < end; i++) {
            ulong value = 1;
            for (const auto &node: children) {
                value *= (node->_children.empty() ? count_leaf(node, i) : count_internal(node, i));
            }
            sum += value;
        }
        return sum;
    }
#endif

    ulong count(const std::shared_ptr<FactorizedTreeElement> &root) {
        const auto &vec = root->_value;
        const auto &children = root->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &curr_size = vec->_state->_state_info._size;

        if (children.empty())
            return curr_size;

        ulong sum = 0;
        for (auto i = 0; i < curr_size; i++) {
            ulong value = 1;
            const auto parent_idx = static_cast<uint32_t>(start_pos + i);
            for (const auto &node: children) {
                value *= (node->_children.empty() ? count_leaf(node, parent_idx) : count_internal(node, parent_idx));
            }
            sum += value;
        }
        return sum;
    }

    void SinkPacked::update_total_row_size_if_materialized() { total_row_size_if_materialized += count(_ftree); }

    void SinkPacked::update_total_column_size_if_materialized() {}

    void dfs_helper(const std::shared_ptr<FactorizedTreeElement> &root, std::unordered_set<std::string> &visited,
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

    void SinkPacked::fill_vectors_in_ftree() const {
        std::unordered_set<std::string> visited;
        dfs_helper(_ftree, visited, _context);
    }

    void SinkPacked::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
    }

    ulong SinkPacked::get_total_row_size_if_materialized() { return total_row_size_if_materialized; }

    ulong SinkPacked::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
