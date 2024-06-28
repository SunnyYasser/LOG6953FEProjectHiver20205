#ifndef SAMPLE_DB_OPERATOR_TYPES_HH
#define SAMPLE_DB_OPERATOR_TYPES_HH

#include <string>

namespace SampleDB
{
    enum operator_type_t 
    {
        OP_GENERIC,
        OP_SCAN,
        OP_INLJ,
        OP_SINK,
        OP_SIZE
    };
}

#endif