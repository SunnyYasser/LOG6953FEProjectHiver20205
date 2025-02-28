#include "include/sink_packed_operator.hh"

#include <cassert>
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

    ulong count_leaf(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state;
        const uint32_t *const __restrict__ rle = state->_rle;
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
#else
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state.get();
        const uint32_t *const __restrict__ rle = state->_rle.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = op->_value;
        const State *const state = vec->_state;
        const uint32_t *const rle = state->_rle;
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
#else
        const Vector *const vec = op->_value;
        const State *const state = vec->_state.get();
        const uint32_t *const rle = state->_rle.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
#endif
#endif

        // For sparse RLE, we only count valid elements
        if (!TEST_BIT(*selection_mask, parent_idx)) {
            return 0;
        }

        // Get the RLE value for this index - number of elements
        // For index i, the number of elements is rle[i+1] - rle[i]
        if (parent_idx == 0) {
            return rle[parent_idx + 1];
        } else {
            return rle[parent_idx + 1] - rle[parent_idx];
        }
    }

    ulong count_internal(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
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
        assert(start_pos <= end_pos);
        assert(start_pos >= 0 && start_pos < State::MAX_VECTOR_SIZE);
        assert(end_pos >= 0 && end_pos < State::MAX_VECTOR_SIZE);
        assert(start_pos >= selection_start_pos && start_pos <= selection_end_pos);
        assert(end_pos >= selection_start_pos && end_pos <= selection_end_pos);
#endif

        // Process all values for this specific parent_idx
        ulong sum = 0;
        const auto children_size = children.size();

        // For this valid parent, calculate product of all children
        for (auto idx = start_pos; idx <= end_pos; idx++) {
            ulong value = 1;
            for (size_t child_idx = 0; child_idx < children_size; child_idx++) {
                const auto &node = children[child_idx];
                if (node->_children.empty()) {
                    value *= count_leaf(node, idx);
                } else {
                    value *= count_internal(node, idx);
                }
            }
            sum += value;
        }
        return sum;
    }

    ulong count(const std::shared_ptr<FactorizedTreeElement> &root) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = root->_value;
        const State *const __restrict__ state = vec->_state;
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
#else
        const Vector *const __restrict__ vec = root->_value;
        const State *const __restrict__ state = vec->_state.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = root->_value;
        const State *const state = vec->_state;
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
#else
        const Vector *const vec = root->_value;
        const State *const state = vec->_state.get();
        const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
#endif
#endif
        const auto &children = root->_children;

        // Get the valid range from the selection mask
        const auto start_pos = GET_START_POS(*selection_mask);
        const auto end_pos = GET_END_POS(*selection_mask);

#ifdef MY_DEBUG
        assert(start_pos <= end_pos);
        assert(start_pos >= 0 && end_pos >= 0 && start_pos < State::MAX_VECTOR_SIZE &&
               end_pos < State::MAX_VECTOR_SIZE);
#endif

        // No children means we just count valid elements between start and end
        // Will be replaced by a faster implementation
        if (children.empty()) {
            ulong count = 0;
            for (auto i = start_pos; i <= end_pos; i++) {
                if (TEST_BIT(*selection_mask, i)) {
                    count++;
                }
            }
            return count;
        }

        // Process each valid index and its children
        ulong sum = 0;
        const auto children_size = children.size();

        for (auto i = start_pos; i <= end_pos; i++) {
            if (!TEST_BIT(*selection_mask, i)) {
                continue;
            }

            ulong value = 1;
            for (size_t child_idx = 0; child_idx < children_size; child_idx++) {
                const auto &node = children[child_idx];
                if (node->_children.empty()) {
                    value *= count_leaf(node, i);
                } else {
                    value *= count_internal(node, i);
                }
            }
            sum += value;
        }

        return sum;
    }

    void SinkPacked::update_total_row_size_if_materialized() { total_row_size_if_materialized += count(_ftree); }

    void SinkPacked::update_total_column_size_if_materialized() {}

    void dfs_helper(const std::shared_ptr<FactorizedTreeElement> &root, std::unordered_set<std::string> &visited,
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

    void SinkPacked::fill_vectors_in_ftree() const {
        std::unordered_set<std::string> visited;
        dfs_helper(_ftree, visited, _context);
    }

    void SinkPacked::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
    }

    ulong SinkPacked::get_total_row_size_if_materialized() { return total_row_size_if_materialized; }

    unsigned long SinkPacked::get_exec_call_counter() const { return _exec_call_counter; }
} // namespace VFEngine
