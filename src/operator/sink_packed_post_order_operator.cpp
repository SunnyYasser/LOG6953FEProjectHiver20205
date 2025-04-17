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
        const auto &selection_mask = *(op->_value->_state->_selection_mask);
        std::array<ulong, State::MAX_VECTOR_SIZE> results{};

        for (size_t i = 0; i < size; ++i) {
            const uint32_t parent_idx = parent_indices[i];

            // Skip invalid indices
            if (!TEST_BIT(selection_mask, parent_idx)) {
                results[i] = 0;
                continue;
            }

            // Calculate RLE difference for this parent
            if (parent_idx == 0) {
                results[i] = rle[parent_idx + 1];
            } else {
                results[i] = rle[parent_idx + 1] - rle[parent_idx];
            }
        }

        return results;
    }

    // For internal nodes
    static std::array<ulong, State::MAX_VECTOR_SIZE>
    count_internal(const std::shared_ptr<FactorizedTreeElement> &op,
                   const std::array<uint32_t, State::MAX_VECTOR_SIZE> &parent_indices, size_t size) {
        const auto &vec = op->_value;
        const auto &children = op->_children;
        const auto &rle = vec->_state->_rle;
        const auto &selection_mask = *(vec->_state->_selection_mask);

        // Get valid range from selection mask
        const auto start_pos = GET_START_POS(selection_mask);
        const auto end_pos = GET_END_POS(selection_mask);

        // Check if there's anything to process
        if (start_pos > end_pos) {
            std::array<ulong, State::MAX_VECTOR_SIZE> empty_result{};
            return empty_result;
        }

        std::array<uint32_t, State::MAX_VECTOR_SIZE> expanded_indices{};
        size_t expanded_size = 0;

        // Create expanded indices only for valid parents
        for (size_t i = 0; i < size; ++i) {
            const uint32_t parent_idx = parent_indices[i];

            // Skip if parent is invalid or out of range
            if (parent_idx < start_pos || parent_idx > end_pos || !TEST_BIT(selection_mask, parent_idx)) {
                continue;
            }

            // Get RLE range for this parent
            uint32_t start, end;
            if (parent_idx == 0) {
                start = 0;
                end = rle[parent_idx + 1];
            } else {
                start = rle[parent_idx];
                end = rle[parent_idx + 1];
            }

            // Only include indices in valid range
            start = std::max(start, static_cast<uint32_t>(start_pos));
            end = std::min(end, static_cast<uint32_t>(end_pos + 1));

            // Add valid indices to expanded array
            for (uint32_t j = start; j < end; ++j) {
                if (TEST_BIT(selection_mask, j)) {
                    expanded_indices[expanded_size++] = j;
                }
            }
        }

        if (expanded_size == 0) {
            std::array<ulong, State::MAX_VECTOR_SIZE> empty_result{};
            return empty_result;
        }

        std::array<ulong, State::MAX_VECTOR_SIZE> combined_results{};
        combined_results.fill(1);

        // Process all children for expanded indices
        for (const auto &child: children) {
            auto child_counts = child->_children.empty() ? count_leaf(child, expanded_indices, expanded_size)
                                                         : count_internal(child, expanded_indices, expanded_size);

            for (size_t i = 0; i < expanded_size; ++i) {
                combined_results[i] *= child_counts[i];
            }
        }

        // Aggregate results back to parent indices
        std::array<ulong, State::MAX_VECTOR_SIZE> results{};
        size_t offset = 0;

        for (size_t i = 0; i < size; ++i) {
            const uint32_t parent_idx = parent_indices[i];

            // Skip invalid parents
            if (parent_idx < start_pos || parent_idx > end_pos || !TEST_BIT(selection_mask, parent_idx)) {
                results[i] = 0;
                continue;
            }

            // Get RLE range
            uint32_t start, end;
            if (parent_idx == 0) {
                start = 0;
                end = rle[parent_idx + 1];
            } else {
                start = rle[parent_idx];
                end = rle[parent_idx + 1];
            }

            // Filter by valid range
            start = std::max(start, static_cast<uint32_t>(start_pos));
            end = std::min(end, static_cast<uint32_t>(end_pos + 1));

            // Count valid children in this range
            uint32_t valid_count = 0;
            for (uint32_t j = start; j < end; ++j) {
                if (TEST_BIT(selection_mask, j)) {
                    valid_count++;
                }
            }

            // Sum results for this parent
            for (uint32_t j = 0; j < valid_count; ++j) {
                results[i] += combined_results[offset++];
            }
        }

        return results;
    }

    // Root count function
    static ulong count(const std::shared_ptr<FactorizedTreeElement> &root) {
        const auto &vec = root->_value;
        const auto &children = root->_children;
        const auto &selection_mask = *(vec->_state->_selection_mask);

        // Get valid range from selection mask
        const auto start_pos = GET_START_POS(selection_mask);
        const auto end_pos = GET_END_POS(selection_mask);

        // Check if there's anything to process
        if (start_pos > end_pos) {
            return 0;
        }

        // For leaf nodes with no children, just count valid bits
        if (children.empty()) {
            ulong count = 0;
            for (auto i = start_pos; i <= end_pos; i++) {
                if (TEST_BIT(selection_mask, i)) {
                    count++;
                }
            }
            return count;
        }

        // Build array of valid parent indices
        std::array<uint32_t, State::MAX_VECTOR_SIZE> parent_indices{};
        size_t valid_count = 0;

        for (auto i = start_pos; i <= end_pos; i++) {
            if (TEST_BIT(selection_mask, i)) {
                parent_indices[valid_count++] = i;
            }
        }

        if (valid_count == 0) {
            return 0;
        }

        // Initialize result array
        std::array<ulong, State::MAX_VECTOR_SIZE> results{};
        results.fill(1);

        // Process all children
        for (const auto &child: children) {
            auto child_counts = child->_children.empty() ? count_leaf(child, parent_indices, valid_count)
                                                         : count_internal(child, parent_indices, valid_count);

            for (size_t i = 0; i < valid_count; ++i) {
                results[i] *= child_counts[i];
            }
        }

        // Sum all results
        return std::accumulate(results.begin(), results.begin() + valid_count, 0UL);
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

    void SinkPackedPostOrder::init(const std::shared_ptr<ContextMemory> &context,
                                   const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
    }

    ulong SinkPackedPostOrder::get_total_row_size_if_materialized() { return total_row_size_if_materialized; }

    ulong SinkPackedPostOrder::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
