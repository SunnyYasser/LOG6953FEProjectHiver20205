#ifndef VFENGINE_LOGICAL_PIPELINE_ELEMENT_H
#define VFENGINE_LOGICAL_PIPELINE_ELEMENT_H

#include "../../operator/include/operator_definition.hh"
#include "../../operator/include/relation_types.hh"
#include "../../operator/include/schema.hh"

namespace VFEngine {

    enum JoinDirection { FORWARD = 0, BACKWARD, ANY };

    struct LogicalPipelineElement {
        operator_type_t operator_type;
        std::string first_col;
        std::string second_col;
        JoinDirection join_direction;
        RelationType relation_type;
    };
} // namespace VFEngine

#endif
