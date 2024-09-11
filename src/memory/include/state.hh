#ifndef VFENGINE_STATE_HH
#define VFENGINE_STATE_HH

#include <cstdint>
#include <memory>

namespace VFEngine {
    class State {
    public:
        State() = delete;

        explicit State(const int32_t &size);

        void print_debug_info() const;

        static constexpr int32_t MAX_VECTOR_SIZE = 1024;

        int32_t _pos;
        int32_t _size;
    };
} // namespace SampleDB
#endif
