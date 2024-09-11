#ifndef VFENGINE_VECTOR_HH
#define VFENGINE_VECTOR_HH

#include <array>
#include <cstdint>
#include <memory>
#include "state.hh"

namespace VFEngine {
    class Vector {
    public:
        Vector();
        explicit Vector(const int32_t &size);
        Vector (const Vector &other) = delete;
        Vector operator= (const Vector &other) = delete;
        void print_debug_info() const;
        std::shared_ptr<State> _state;
        uint64_t *_values;

    private:
        std::unique_ptr<uint64_t[]> _values_uptr;
    };
} // namespace VFEngine

#endif
