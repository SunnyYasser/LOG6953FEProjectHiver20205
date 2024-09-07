#ifndef VFENGINE_VECTOR_HH
#define VFENGINE_VECTOR_HH

#include <cstdint>
#include <memory>
#include <vector>
#include "state.hh"

namespace VFEngine {
    class Vector {
    public:
        Vector();

        explicit Vector(const int32_t &size);

        explicit Vector(const std::vector<int32_t> &vector);

        void print_debug_info() const;

        std::vector<int32_t> _vector;
        std::shared_ptr<State> _state;
    };
} // namespace VFEngine

#endif
