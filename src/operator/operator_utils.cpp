#include "include/operator_utils.hh"
#include <unordered_set>

namespace VFEngine {

    void remove_duplicates(std::vector<uint64_t> vec, std::vector<uint64_t> &_attribute_data) {
        std::unordered_set<uint64_t> set{begin(vec), end(vec)};
        _attribute_data = {begin(set), end(set)};
    }

    bool should_enable_state_sharing(const RelationType &relation_type, const bool &is_join_index_fwd) {
        return relation_type == RelationType::ONE_TO_ONE or
               (relation_type == RelationType::MANY_TO_ONE and is_join_index_fwd) or
               (relation_type == RelationType::ONE_TO_MANY and !is_join_index_fwd);
    }

    bool is_packed_operator(const Operator *op) {
        return op->get_operator_type() == OP_SINK_PACKED or op->get_operator_type() == OP_SINK_PACKED_MIN or
               op->get_operator_type() == OP_SINK_PACKED_HARDCODED_LINEAR;
    }


} // namespace VFEngine
