#include "include/vector.hh"
#include <algorithm>
#include <iostream>

namespace VFEngine {
    Vector::Vector() :
        _state(std::make_shared<State>(State::MAX_VECTOR_SIZE)),
        _values_uptr(std::make_unique<uint64_t[]>(State::MAX_VECTOR_SIZE)) {
        _values = _values_uptr.get();
    }

    Vector::Vector(const int32_t &size) :
        _state(std::make_shared<State>(size)), _values_uptr(std::make_unique<uint64_t[]>(State::MAX_VECTOR_SIZE)) {
        _values = _values_uptr.get();
    }

    void Vector::print_debug_info() const {
        std::cout << "VECTOR DEBUG INFO BEGINS:\n";
        _state->print_debug_info();

        std::cout << "VECTOR DATA VALUES:\n";

        for (int32_t idx = 0; idx < _state->_size; idx++) {
            std::cout << _values[idx] << "\n";
        }

        std::cout << "VECTOR DEBUG INFO ENDS:\n";
    }

} // namespace VFEngine
