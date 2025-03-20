#ifndef VFENGINE_OPERATOR_TYPES_HH
#define VFENGINE_OPERATOR_TYPES_HH

namespace VFEngine {
    enum operator_type_t {
        OP_GENERIC,
        OP_SCAN,
        OP_SCAN_FAILURE_PROP,
        OP_INLJ,
        OP_INLJ_PACKED,
        OP_INLJ_NTO1,
        OP_SINK,
        OP_SINK_PACKED,
        OP_SINK_NO_OP,
        OP_SINK_PACKED_VECTORIZED,
        OP_SINK_PACKED_HARDCODED_LINEAR,
        OP_SINK_PACKED_MIN,
        OP_CASCADE_SELECTION,
        OP_SINK_FAILURE_PROP,
        OP_SIZE
    };
}

#endif
