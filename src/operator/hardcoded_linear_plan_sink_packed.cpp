#include "include/hardcoded_linear_plan_sink_packed.hh"

#include <cassert>
#include <unordered_set>
#include <vector>

namespace VFEngine {
    ulong SinkLinearHardcoded::total_row_size_if_materialized = 0;

    SinkLinearHardcoded::SinkLinearHardcoded(const std::shared_ptr<FactorizedTreeElement> &ftree) :
        Operator(), _ftree(ftree), _leaf_state(nullptr) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t SinkLinearHardcoded::get_operator_type() const { return OP_SINK_PACKED_HARDCODED_LINEAR; }

    void SinkLinearHardcoded::execute() {
        _exec_call_counter++;
        update_total_row_size_if_materialized();
    }

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
    static ulong count(const State *leaf) {
#else
    static ulong count(const std::shared_ptr<State> &leaf) {
#endif
        const auto &rle = leaf->_rle;
        const auto &selection_mask = *(leaf->_selection_mask);

        // Get the valid range from the selection mask
        const auto start_pos = GET_START_POS(selection_mask);
        const auto end_pos = GET_END_POS(selection_mask);

        // Check if there's any valid range
        if (start_pos > end_pos) {
            return 0;
        }

        // Count valid elements using RLE
        ulong total_count = 0;

        for (auto idx = start_pos; idx <= end_pos; idx++) {
            // Skip invalid indices
            if (!TEST_BIT(selection_mask, idx)) {
                continue;
            }

            // Calculate the number of elements at this index
            uint32_t count_at_idx;
            if (idx == 0) {
                count_at_idx = rle[idx + 1];
            } else {
                count_at_idx = rle[idx + 1] - rle[idx];
            }

            total_count += count_at_idx;
        }

        return total_count;
    }

    void SinkLinearHardcoded::update_total_row_size_if_materialized() {
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        total_row_size_if_materialized += count(_leaf_state);
#else
        total_row_size_if_materialized += count(*_leaf_state);
#endif
    }

    void SinkLinearHardcoded::update_total_column_size_if_materialized() {}

    static void dfs_helper(const std::shared_ptr<FactorizedTreeElement> &root, std::unordered_set<std::string> &visited,
                           const std::shared_ptr<ContextMemory> &context) {
        const auto attr = root->_attribute;
        if (visited.contains(attr)) {
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

    void SinkLinearHardcoded::fill_vectors_in_ftree() const {
        std::unordered_set<std::string> visited;
        dfs_helper(_ftree, visited, _context);
    }

    void SinkLinearHardcoded::capture_leaf_node(const std::shared_ptr<FactorizedTreeElement> &root) {
        if (root->_children.empty()) {
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
            _leaf_state = root->_value->_state;
#else
            _leaf_state = &(root->_value->_state);
#endif
            return;
        }
        assert(root->_children.size() == 1 && "Node has more than one child");
        capture_leaf_node(root->_children[0]);
    }

    void SinkLinearHardcoded::init(const std::shared_ptr<ContextMemory> &context,
                                   const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
        capture_leaf_node(_ftree);
    }

    ulong SinkLinearHardcoded::get_total_row_size_if_materialized() { return total_row_size_if_materialized; }

    ulong SinkLinearHardcoded::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
