#ifndef VFENGINE_VECTOR_HH
#define VFENGINE_VECTOR_HH

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <fstream>
#include "state.hh"

namespace VFEngine {
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
        bool *_filter;

    private:
        /*
         * Using uint8_t as underlying type since its easier to access bytes of memory (see c++ strict aliasing rules)
         * We need to provide a custom deleter for the uptr, since we are using std::aligned_alloc, and it requires use
         * of std::free(). we use decltype to capture the function signature, and is equivalent to
         * std::unique_ptr<uint8_t[], void(*)(void*)> _data_uptr. We only need to declare the signature, not re define
         * std::free since it is a primitive type
         */
        std::unique_ptr<uint8_t[], decltype(&std::free)> _data_uptr;
        void set_default_values();
    };
} // namespace VFEngine

#endif
