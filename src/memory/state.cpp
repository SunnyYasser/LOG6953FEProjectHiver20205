#include <iostream>
#include "include/state.hh"

namespace VFEngine {
    State::State(const int32_t &size) : _pos(-1), _size(size) {
    }

    int32_t State::get_pos() const {
        return _pos;
    }

    int32_t State::get_size() const {
        return _size;
    }

    void State::update_pos() {
        _pos++;
    }

    void State::update_size() {
        _size++;
    }

    void State::print_debug_info() const {
        std::cout << "[STATE pos]: " << _pos << std::endl;
        std::cout << "[STATE size]: " << _size << std::endl;
    }

    void State::set_pos(const int32_t &pos) {
        _pos = pos;
    }

    void State::set_size(const int32_t &size) {
        _size = size;
    }

}
