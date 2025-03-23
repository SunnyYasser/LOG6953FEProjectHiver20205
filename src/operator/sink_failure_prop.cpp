#include "include/sink_failure_prop.hh"

#include <cassert>
#include <queue>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    SinkFailureProp::SinkFailureProp(const std::shared_ptr<FactorizedTreeElement> &ftree) :
        Operator(), _ftree(ftree), _total_rows(nullptr) {}

    operator_type_t SinkFailureProp::get_operator_type() const { return OP_SINK_FAILURE_PROP; }

    void SinkFailureProp::execute() {
        _exec_call_counter++;
        update_total_rows();
    }

    static std::vector<uint64_t> collect_values_at_level(const std::shared_ptr<FactorizedTreeElement> &node, int level,
                                                         int target_level) {

        // Base case: we've reached our target level
        if (level == target_level) {
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
            const Vector *const __restrict__ vec = node->_value;
            const State *const __restrict__ state = vec->_state;
            const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
            const uint64_t *const __restrict__ values = vec->_values;
#else
            const Vector *const __restrict__ vec = node->_value;
            const State *const __restrict__ state = vec->_state.get();
            const BitMask<State::MAX_VECTOR_SIZE> *const __restrict__ selection_mask = state->_selection_mask;
            const uint64_t *const __restrict__ values = vec->_values;
#endif
#else
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
            const Vector *const vec = node->_value;
            const State *const state = vec->_state;
            const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
            const uint64_t *const values = vec->_values;
#else
            const Vector *const vec = node->_value;
            const State *const state = vec->_state.get();
            const BitMask<State::MAX_VECTOR_SIZE> *const selection_mask = state->_selection_mask;
            const uint64_t *const values = vec->_values;
#endif
#endif

            std::vector<uint64_t> result;

            // Get the valid range from the selection mask
            const auto start_pos = 0;
            const auto end_pos = state->_state_info._size - 1;

            // Collect all valid values at this level
            for (auto i = start_pos; i <= end_pos; i++) {
                result.push_back(values[i]);
            }

            return result;
        }

        // If we're not at the target level, recursively check children
        std::vector<uint64_t> result;
        for (const auto &child: node->_children) {
            auto child_values = collect_values_at_level(child, level + 1, target_level);
            result.insert(result.end(), child_values.begin(), child_values.end());
        }

        return result;
    }

    static int get_tree_height(const std::shared_ptr<FactorizedTreeElement> &node) {
        if (node->_children.empty()) {
            return 1;
        }

        int max_height = 0;
        for (const auto &child: node->_children) {
            max_height = std::max(max_height, get_tree_height(child));
        }

        return 1 + max_height;
    }

    void SinkFailureProp::update_total_rows() {
        // First, get the height of the tree
        const int tree_height = get_tree_height(_ftree);

        // Collect values level by level (BFS manner)
        for (int level = 0; level < tree_height; level++) {
            std::vector<uint64_t> level_values = collect_values_at_level(_ftree, 0, level);

            // If we got values at this level, add them to total_rows
            if (!level_values.empty()) {
                _total_rows->push_back(level_values);
            }
        }

        // Add a demarcation row filled with -1
        std::vector<uint64_t> demarcation_row(State::MAX_VECTOR_SIZE, 0);
        _total_rows->push_back(demarcation_row);
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
        _total_rows = std::make_unique<std::vector<std::vector<uint64_t>>>();
    }

    std::vector<std::vector<uint64_t>> SinkFailureProp::get_total_rows() const { return *_total_rows; }

    unsigned long SinkFailureProp::get_exec_call_counter() const { return _exec_call_counter; }
} // namespace VFEngine
