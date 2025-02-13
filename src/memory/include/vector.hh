#ifndef VFENGINE_VECTOR_HH
#define VFENGINE_VECTOR_HH

#include <cstdint>
#include <cstdlib>
#include <memory>
#include "state.hh"

namespace VFEngine {
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
    class Vector {
    public:
        Vector();
        explicit Vector(const int32_t &size);
        explicit Vector(const State *state);

        void print_debug_info(std::ofstream &logfile) const;
        Vector(const Vector &other) = delete;
        Vector operator=(const Vector &other) = delete;
        State *_state;
        uint64_t *_values;
        void allocate_rle() const;
        void allocate_filter() const;

    private:
        void set_default_values() const;
    };
#else
    class Vector {
    public:
        Vector();
        explicit Vector(const int32_t &size);
        explicit Vector(const std::shared_ptr<State> &state);
        void print_debug_info(std::ofstream &logfile) const;
        Vector(const Vector &other) = delete;
        Vector operator=(const Vector &other) = delete;
        std::shared_ptr<State> _state;
        uint64_t *_values;
        void allocate_rle() const;
        void allocate_filter() const;
    private:
        std::unique_ptr<uint64_t[]> _values_ptr;
        void set_default_values() const;
    };
#endif
} // namespace VFEngine

#endif
