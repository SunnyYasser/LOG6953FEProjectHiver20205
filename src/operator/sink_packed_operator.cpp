#include "include/sink_packed_operator.hh"
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
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state;
        const uint32_t *const __restrict__ rle = state->_rle;
#else
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state.get();
        const uint32_t *const __restrict__ rle = state->_rle.get();
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = op->_value;
        const State *const state = vec->_state;
        const uint32_t *const rle = state->_rle;
#else
        const Vector *const vec = op->_value;
        const State *const state = vec->_state.get();
        const uint32_t *const rle = state->_rle.get();
#endif
#endif

#ifdef MEMSET_TO_SET_VECTOR_SLICE
        return rle[parent_idx + 1] - rle[parent_idx];
#else
        const auto rle_start_pos = state->_rle_start_pos;
        return (parent_idx + 1 >= rle_start_pos) *
               (rle[parent_idx + 1] - rle[parent_idx] * (parent_idx + 1 > rle_start_pos));
#endif
    }
#else
    ulong count_leaf(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state;
        const uint32_t *const __restrict__ rle = state->_rle;
#else
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state.get();
        const uint32_t *const __restrict__ rle = state->_rle.get();
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = op->_value;
        const State *const state = vec->_state;
        const uint32_t *const rle = state->_rle;
#else
        const Vector *const vec = op->_value;
        const State *const state = vec->_state.get();
        const uint32_t *const rle = state->_rle.get();
#endif
#endif

#ifdef MEMSET_TO_SET_VECTOR_SLICE
        return rle[parent_idx + 1] - rle[parent_idx];
#else
        const auto rle_start_pos = state->_rle_start_pos;
        if (parent_idx + 1 < rle_start_pos)
            return 0;
        return parent_idx + 1 > rle_start_pos ? rle[parent_idx + 1] - rle[parent_idx] : rle[parent_idx + 1];
#endif
    }
#endif

    ulong count_internal(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state;
        const uint32_t *const __restrict__ rle = state->_rle;
#else
        const Vector *const __restrict__ vec = op->_value;
        const State *const __restrict__ state = vec->_state.get();
        const uint32_t *const __restrict__ rle = state->_rle.get();
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = op->_value;
        const State *const state = vec->_state;
        const uint32_t *const rle = state->_rle;
#else
        const Vector *const vec = op->_value;
        const State *const state = vec->_state.get();
        const uint32_t *const rle = state->_rle.get();
#endif
#endif
        const auto &children = op->_children;

        const auto start_pos = state->_state_info._curr_start_pos;
        const auto size = state->_state_info._size;

#ifdef MEMSET_TO_SET_VECTOR_SLICE
        const auto start = std::max(rle[parent_idx], static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);
#else
        const auto rle_start_pos = state->_rle_start_pos;
        const auto rle_val = parent_idx == rle_start_pos - 1 ? 0 : rle[parent_idx];
        const auto start = std::max(rle_val, static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);
#endif

        ulong sum = 0;
        const auto children_size = children.size();

        for (auto i = start; i < end; i++) {
            ulong value = 1;
            for (size_t child_idx = 0; child_idx < children_size; child_idx++) {
                const auto &node = children[child_idx];
                value *= node->_children.empty() ? count_leaf(node, i) : count_internal(node, i);
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
#else
        const Vector *const __restrict__ vec = root->_value;
        const State *const __restrict__ state = vec->_state.get();
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        const Vector *const vec = root->_value;
        const State *const state = vec->_state;
#else
        const Vector *const vec = root->_value;
        const State *const state = vec->_state.get();
#endif
#endif
        const auto &children = root->_children;

        const auto start_pos = state->_state_info._curr_start_pos;
        const auto curr_size = state->_state_info._size;

        if (children.empty())
            return curr_size;

        ulong sum = 0;
        const auto children_size = children.size();

        for (auto i = 0; i < curr_size; i++) {
            ulong value = 1;
            const auto parent_idx = static_cast<uint32_t>(start_pos + i);

            for (size_t child_idx = 0; child_idx < children_size; child_idx++) {
                const auto &node = children[child_idx];
                value *= node->_children.empty() ? count_leaf(node, parent_idx) : count_internal(node, parent_idx);
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

    ulong SinkPacked::get_exec_call_counter() const { return _exec_call_counter; }
} // namespace VFEngine
