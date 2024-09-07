#include <iostream>
#include "include/state.hh"

namespace VFEngine {
    State::State(const int32_t &size) : _pos(-1), _size(size) {
    }

    void State::print_debug_info() const {
        std::cout << "[STATE pos]: " << _pos << std::endl;
        std::cout << "[STATE size]: " << _size << std::endl;
    }

}
