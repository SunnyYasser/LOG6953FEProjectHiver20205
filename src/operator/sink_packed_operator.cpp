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

    /*
     * Idea behind sink packed is as follows
     * 1) If the vector is the root of ftree, its RLE will not be available. Instead we use the curr_start_pos and the
     * curr_size, which would have been set last by some operator in the ftree
     * 2) Each element in the root will lead to the output of N number of tuples, where N >= 0
     * 3) We do a dfs call to the children of the current operator, we also pass the chunk_idx, which defines
     * for which current parent element are we processing the current vector
     * 4) Generally, for each element in a parent vector, we have a chunk of elements in the child vector, and the
     * chunk_idx stores this relationship
     * 5) For leaf nodes, we do not have to the recursive dfs call, and therefore simply return the size of the rle
     * chunk pointed to by chunk_idx
     * 6) Comments are added in the code as well for each specific line of interest
     */


    ulong count(const std::shared_ptr<FactorizedTreeElement> &op, const uint32_t parent_idx) {
#ifdef MY_DEBUG
        const auto attr = op->_attribute;
        const auto &rle_size = op->_value->_state->_rle_size;
#endif

        const auto &vec = op->_value;
        const auto &children = op->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &size = vec->_state->_state_info._size;
        const auto &rle = vec->_state->_rle; // should have _rle[0] = 1

        if (children.size() == 0) {
            return rle[parent_idx + 1] - rle[parent_idx];
        }

        // for each set of a's, when we reach sink, not all c's would be produced, only a slice of the subarray of the
        // 'a' vector is producing c's right now. We need to find the correct slice limits
        const auto start = std::max(rle[parent_idx], static_cast<uint32_t>(start_pos));
        const auto end = std::min(rle[parent_idx + 1], static_cast<uint32_t>(start_pos) + size);

        ulong sum = 0u;
        for (auto i = start; i < end; i++) {
            ulong value = 1;
            for (const auto &node: children) {
#ifdef MY_DEBUG
                const auto &child_attr = node->_attribute;
                const auto &child_vec = node->_value;
                const auto &child_size = child_vec->_state->_state_info._size;
                const auto &child_start_pos = child_vec->_state->_state_info._curr_start_pos;
                const auto &child_rle = child_vec->_state->_rle; // should have _rle[0] = 1
#endif

                value *= count(node, i);
            }
            sum += value;
        }
        return sum;
    }

    ulong count(const std::shared_ptr<FactorizedTreeElement> &root) {
        ulong sum = 0u;
        const auto &vec = root->_value;
        const auto &children = root->_children;
        const auto &start_pos = vec->_state->_state_info._curr_start_pos;
        const auto &curr_size = vec->_state->_state_info._size;

        if (children.empty())
            return curr_size;

        // loop over root and its children
        for (auto i = 0; i < curr_size; i++) {
            ulong value = 1u;
            const auto parent_idx = start_pos + i;
            for (const auto &node: children) {
#ifdef MY_DEBUG
                const auto child_attr = node->_attribute;
                const auto &child_vec = node->_value;
                const auto &child_size = child_vec->_state->_state_info._size;
                auto child_start_pos = child_vec->_state->_state_info._curr_start_pos;
                const auto &child_rle = child_vec->_state->_rle; // should have _rle[0] = 1
#endif
                value *= count(node, parent_idx);
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


    /*
     * We only need to create read the data of given column (attribute)
     * Output vector is not required for sink operator
     */
    void SinkPacked::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        _context = context;
        fill_vectors_in_ftree();
    }


    ulong SinkPacked::get_total_row_size_if_materialized() { return total_row_size_if_materialized; }

    ulong SinkPacked::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
