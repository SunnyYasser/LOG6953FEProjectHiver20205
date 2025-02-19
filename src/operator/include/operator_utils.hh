#ifndef VFENGINE_OPERATOR_UTILS_HH
#define VFENGINE_OPERATOR_UTILS_HH

#include "operator_definition.hh"
#include "relation_types.hh"

namespace VFEngine {
    void remove_duplicates(std::vector<uint64_t> vec, std::vector<uint64_t> &_attribute_data);
    bool should_enable_state_sharing(const RelationType &relation_type, const bool &is_join_index_fwd);
    bool is_packed_operator(const Operator *op);
} // namespace VFEngine

#endif
