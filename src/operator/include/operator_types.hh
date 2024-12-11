#ifndef VFENGINE_OPERATOR_TYPES_HH
#define VFENGINE_OPERATOR_TYPES_HH

namespace VFEngine {
    enum operator_type_t {
        OP_GENERIC,
        OP_SCAN,
        OP_INLJ,
        OP_INLJ_PACKED,
        OP_INLJ_NTO1,
        OP_SINK,
        OP_SINK_PACKED,
        OP_SIZE
    };
}

#endif
