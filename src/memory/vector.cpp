#include <cassert>
#include <iostream>
#include <algorithm>
#include "include/vector.hh"

namespace VFEngine {
    Vector::Vector() : _vector(std::vector<uint64_t>(State::MAX_VECTOR_SIZE, 0)),
                       _state(std::make_shared<State>(State::MAX_VECTOR_SIZE)) {
    }

    Vector::Vector(const std::vector<uint64_t> &vector) : _vector(vector),
                                                         _state(std::make_shared<State>(
                                                             std::min(vector.size(), State::MAX_VECTOR_SIZE))) {
    }

    Vector::Vector(const int32_t &size) : _vector(std::vector<uint64_t>(std::min(static_cast<int32_t>(State::MAX_VECTOR_SIZE), size),
                                                                       0)),
                                          _state(std::make_shared<State>(
                                              std::min(size, static_cast<int32_t>(State::MAX_VECTOR_SIZE)))) {
    }


    void Vector::print_debug_info() const {
        std::cout << "VECTOR DEBUG INFO BEGINS:\n";
        _state->print_debug_info();

        std::cout << "VECTOR DATA VALUES:\n";

        for (auto &v: _vector) {
            std::cout << v << "\n";
        }

        std::cout << "VECTOR DEBUG INFO ENDS:\n";
    }

}
