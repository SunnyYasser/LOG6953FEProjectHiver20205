#include "include/sink_packed_post_order_operator.hh"
#include <algorithm>
#include <array>
#include <numeric>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    ulong SinkPackedPostOrder::total_row_size_if_materialized = 0;

    SinkPackedPostOrder::SinkPackedPostOrder(const std::shared_ptr<FactorizedTreeElement> &ftree) :
        Operator(), _ftree(ftree) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t SinkPackedPostOrder::get_operator_type() const { return OP_SINK_PACKED_VECTORIZED; }

    void SinkPackedPostOrder::execute() {
        _exec_call_counter++;
        update_total_row_size_if_materialized();
    }

    // For leaf nodes
    static std::array<ulong, State::MAX_VECTOR_SIZE>
    count_leaf(const std::shared_ptr<FactorizedTreeElement> &op,
               const std::array<uint32_t, State::MAX_VECTOR_SIZE> &parent_indices, size_t size) {
        const auto &rle = op->_value->_state->_rle;
        std::array<ulong, State::MAX_VECTOR_SIZE> results{};

        for (size_t i = 0; i < size; ++i) {
            results[i] = rle[parent_indices[i] + 1] - rle[parent_indices[i]];
        }

        return results;
    }

    // For internal nodes
    static std::array<ulong, State::MAX_VECTOR_SIZE>
    count_internal(const std::shared_ptr<FactorizedTreeElement> &op,
                   const std::array<uint32_t, State::MAX_VECTOR_SIZE> &parent_indices, size_t size) {
        const auto &vec = op->_value;
        const auto &children = op->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &size_limit = vec->_state->_state_info._size;
        const auto &rle = vec->_state->_rle;

        std::array<uint32_t, State::MAX_VECTOR_SIZE> expanded_indices{};
        size_t expanded_size = 0;

        for (size_t i = 0; i < size; ++i) {
            const auto start = std::max(rle[parent_indices[i]], static_cast<uint32_t>(start_pos));
            const auto end = std::min(rle[parent_indices[i] + 1], static_cast<uint32_t>(start_pos) + size_limit);
            for (uint32_t j = start; j < end; ++j) {
                expanded_indices[expanded_size++] = j;
            }
        }

        if (expanded_size == 0) {
            std::array<ulong, State::MAX_VECTOR_SIZE> empty_result{};
            return empty_result;
        }

        std::array<ulong, State::MAX_VECTOR_SIZE> combined_results{};
        combined_results.fill(1);

        for (const auto &child: children) {
            auto child_counts = child->_children.empty() ? count_leaf(child, expanded_indices, expanded_size)
                                                         : count_internal(child, expanded_indices, expanded_size);
            for (size_t i = 0; i < expanded_size; ++i) {
                combined_results[i] *= child_counts[i];
            }
        }

        std::array<ulong, State::MAX_VECTOR_SIZE> results{};
        size_t offset = 0;
        for (size_t i = 0; i < size; ++i) {
            const auto start = std::max(rle[parent_indices[i]], static_cast<uint32_t>(start_pos));
            const auto end = std::min(rle[parent_indices[i] + 1], static_cast<uint32_t>(start_pos) + size_limit);
            for (uint32_t j = start; j < end; ++j) {
                results[i] += combined_results[offset++];
            }
        }

        return results;
    }

    // Root count function
    static ulong count(const std::shared_ptr<FactorizedTreeElement> &root) {
        const auto &vec = root->_value;
        const auto &children = root->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &curr_size = vec->_state->_state_info._size;

        if (children.empty()) {
            return curr_size;
        }

        std::array<uint32_t, State::MAX_VECTOR_SIZE> parent_indices{};
        for (size_t i = 0; i < curr_size; ++i) {
            parent_indices[i] = start_pos + i;
        }

        std::array<ulong, State::MAX_VECTOR_SIZE> results{};
        results.fill(1);

        for (const auto &child: children) {
            auto child_counts = child->_children.empty() ? count_leaf(child, parent_indices, curr_size)
                                                         : count_internal(child, parent_indices, curr_size);
            for (size_t i = 0; i < curr_size; ++i) {
                results[i] *= child_counts[i];
            }
        }

        return std::accumulate(results.begin(), results.begin() + curr_size, 0UL);
    }


    void SinkPackedPostOrder::update_total_row_size_if_materialized() {
        total_row_size_if_materialized += count(_ftree);
    }

    void SinkPackedPostOrder::update_total_column_size_if_materialized() {}

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
    void SinkPackedPostOrder::fill_vectors_in_ftree() const {
        std::unordered_set<std::string> visited;
        dfs_helper(_ftree, visited, _context);
    }


    /*
     * We only need to create read the data of given column (attribute)
     * Output vector is not required for sink operator
     */
    void SinkPackedPostOrder::init(const std::shared_ptr<ContextMemory> &context,
                                   const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
    }


    ulong SinkPackedPostOrder::get_total_row_size_if_materialized() { return total_row_size_if_materialized; }

    ulong SinkPackedPostOrder::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
