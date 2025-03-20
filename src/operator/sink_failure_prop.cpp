#include "include/sink_failure_prop.hh"

#include <cassert>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    SinkFailureProp::SinkFailureProp(const std::shared_ptr<FactorizedTreeElement> &ftree) : Operator(), _ftree(ftree) {}

    operator_type_t SinkFailureProp::get_operator_type() const { return OP_SINK_FAILURE_PROP; }

    void SinkFailureProp::execute() {
        _exec_call_counter++;
        update_total_rows();
    }

    static std::vector<uint64_t> collect_leaf_values(const std::shared_ptr<FactorizedTreeElement> &op,
                                                     const uint32_t parent_idx) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state;
        const uint32_t *const __restrict__ rle = state->_rle;
        const uint64_t *const __restrict__ values = vec->_values;
#else
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state.get();
        const uint32_t *const __restrict__ rle = state->_rle.get();
        const uint64_t *const __restrict__ values = vec->_values;
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = op->_value;
        const State *const state = vec->_state;
        const uint32_t *const rle = state->_rle;
        const uint64_t *const values = vec->_values;
#else
        const Vector *const vec = op->_value;
        const State *const state = vec->_state.get();
        const uint32_t *const rle = state->_rle.get();
        const uint64_t *const values = vec->_values;
#endif
#endif

        std::vector<uint64_t> result;
        // Get the start and count from RLE
        const uint32_t start_pos = rle[parent_idx];
        const uint32_t count = rle[parent_idx + 1] - rle[parent_idx];

        // Collect all values for this leaf node at parent_idx
        for (uint32_t i = 0; i < count; i++) {
            const uint32_t idx = start_pos + i;
            result.push_back(values[idx]);
        }

        return result;
    }

    static std::vector<std::vector<uint64_t>> collect_internal_values(const std::shared_ptr<FactorizedTreeElement> &op,
                                                                      const uint32_t parent_idx) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state;
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
        const uint32_t *const rle = state->_rle;
#else
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
        const uint32_t *const rle = state->_rle.get();
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = op->_value;
        const State *const state = vec->_state;
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
        const uint32_t *const rle = state->_rle;
#else
        const Vector *const vec = op->_value;
        const State *const state = vec->_state.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
        const uint32_t *const rle = state->_rle.get();
#endif
#endif
        const auto &children = op->_children;

        std::vector<std::vector<uint64_t>> result;

        // Get the valid range from the selection mask
        const auto selection_start_pos = GET_START_POS(*selection_mask);
        const auto selection_end_pos = GET_END_POS(*selection_mask);
        const auto start_pos = rle[parent_idx];
        const auto num_elems = rle[parent_idx + 1] - rle[parent_idx];
        const auto end_pos = start_pos + num_elems - 1;

#ifdef MY_DEBUG
        assert(selection_start_pos <= selection_end_pos);
        assert(selection_start_pos >= 0 && selection_start_pos < State::MAX_VECTOR_SIZE);
        assert(selection_end_pos >= 0 && selection_end_pos < State::MAX_VECTOR_SIZE);
        assert(start_pos >= 0 && start_pos < State::MAX_VECTOR_SIZE);
        assert(end_pos >= 0 && end_pos < State::MAX_VECTOR_SIZE);
#endif

        // Set iteration limits
        const auto start_val = std::max(static_cast<int32_t>(start_pos), selection_start_pos);
        const auto end_val = std::min(static_cast<int32_t>(end_pos), selection_end_pos);
        const auto children_size = children.size();

        // For each valid index in the range
        for (auto idx = start_val; idx <= end_val; idx++) {
            if (!TEST_BIT(*selection_mask, idx)) {
                continue;
            }

            // For each valid index, collect all combinations of child values
            std::vector<std::vector<uint64_t>> child_values;
            for (size_t child_idx = 0; child_idx < children_size; child_idx++) {
                const auto &node = children[child_idx];
                if (node->_children.empty()) {
                    // Leaf node: collect values directly
                    std::vector<uint64_t> leaf_values = collect_leaf_values(node, idx);
                    if (!leaf_values.empty()) {
                        child_values.push_back(leaf_values);
                    }
                } else {
                    // Internal node: collect values recursively
                    std::vector<std::vector<uint64_t>> internal_results = collect_internal_values(node, idx);
                    // Flatten the results
                    for (const auto &internal_result: internal_results) {
                        if (!internal_result.empty()) {
                            child_values.push_back(internal_result);
                        }
                    }
                }
            }

            // If we have child values, add them to the result
            if (!child_values.empty()) {
                for (const auto &values: child_values) {
                    result.push_back(values);
                }
            }
        }

        return result;
    }

    static std::vector<std::vector<uint64_t>> collect_values(const std::shared_ptr<FactorizedTreeElement> &root) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = root->_value;
        const State *const __restrict__ state = vec->_state;
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
        const uint64_t *const __restrict__ values = vec->_values;
#else
        const Vector *const __restrict__ vec = root->_value;
        const State *const __restrict__ state = vec->_state.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
        const uint64_t *const __restrict__ values = vec->_values;
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = root->_value;
        const State *const state = vec->_state;
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
        const uint64_t *const values = vec->_values;
#else
        const Vector *const vec = root->_value;
        const State *const state = vec->_state.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
        const uint64_t *const values = vec->_values;
#endif
#endif
        const auto &children = root->_children;

        std::vector<std::vector<uint64_t>> result;

        // Get the valid range from the selection mask
        const auto start_pos = GET_START_POS(*selection_mask);
        const auto end_pos = GET_END_POS(*selection_mask);
        const auto children_size = children.size();

#ifdef MY_DEBUG
        assert(start_pos <= end_pos);
        assert(start_pos >= 0 && start_pos < State::MAX_VECTOR_SIZE);
        assert(end_pos >= 0 && end_pos < State::MAX_VECTOR_SIZE);
#endif

        // Process each valid index in the root
        for (auto i = start_pos; i <= end_pos; i++) {
            if (!TEST_BIT(*selection_mask, i)) {
                continue;
            }

            // For each valid index in the root, collect all results from its children
            std::vector<std::vector<uint64_t>> root_value_results;

            // Store the root value
            uint64_t root_value = values[i];
            std::vector<uint64_t> current_row;
            current_row.push_back(root_value);

            // For each child of the root
            for (size_t child_idx = 0; child_idx < children_size; child_idx++) {
                const auto &node = children[child_idx];
                if (node->_children.empty()) {
                    // Leaf node: collect values directly
                    std::vector<uint64_t> leaf_values = collect_leaf_values(node, i);
                    for (uint64_t value: leaf_values) {
                        std::vector<uint64_t> row = current_row;
                        row.push_back(value);
                        root_value_results.push_back(row);
                    }
                } else {
                    // Internal node: collect values recursively
                    std::vector<std::vector<uint64_t>> internal_results = collect_internal_values(node, i);
                    for (const auto &internal_result: internal_results) {
                        std::vector<uint64_t> row = current_row;
                        row.insert(row.end(), internal_result.begin(), internal_result.end());
                        root_value_results.push_back(row);
                    }
                }
            }

            // Add all combinations for this root value to the result
            for (const auto &row: root_value_results) {
                result.push_back(row);
            }
        }

        return result;
    }


    void SinkFailureProp::update_total_rows() {
        // Collect all values from the factorized tree
        std::vector<std::vector<uint64_t>> results = collect_values(_ftree);

        // Add all results to total_rows
        for (const auto &row: results) {
            total_rows.push_back(row);
        }

        // Add a demarcation row filled with -1
        std::vector<uint64_t> demarcation_row(State::MAX_VECTOR_SIZE, -1);
        total_rows.push_back(demarcation_row);
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

    void SinkFailureProp::fill_vectors_in_ftree() const {
        std::unordered_set<std::string> visited;
        dfs_helper(_ftree, visited, _context);
    }

    void SinkFailureProp::init(const std::shared_ptr<ContextMemory> &context,
                               const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
    }

    std::vector<std::vector<uint64_t>> SinkFailureProp::get_total_rows() const { return total_rows; }

    unsigned long SinkFailureProp::get_exec_call_counter() const { return _exec_call_counter; }
} // namespace VFEngine
