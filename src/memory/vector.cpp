#include "include/vector.hh"
#include <cstring>
#include <iostream>
#include "../graph/include/arena_allocator.hh"

namespace VFEngine {
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
    Vector::Vector() :
        _state(new(ArenaAllocator::getInstance().allocate(sizeof(State))) State(State::MAX_VECTOR_SIZE)) {

        _values = static_cast<uint64_t *>(
                ArenaAllocator::getInstance().allocate(State::MAX_VECTOR_SIZE * sizeof(uint64_t)));
        set_default_values();
    }

    Vector::Vector(const int32_t &size) :
        _state(new(ArenaAllocator::getInstance().allocate(sizeof(State))) State(size)) {

        _values = static_cast<uint64_t *>(
                ArenaAllocator::getInstance().allocate(State::MAX_VECTOR_SIZE * sizeof(uint64_t)));
        set_default_values();
    }

    Vector::Vector(const State *state) : _state(const_cast<State *>(state)) {
        _values = static_cast<uint64_t *>(
                ArenaAllocator::getInstance().allocate(State::MAX_VECTOR_SIZE * sizeof(uint64_t)));
        set_default_values();
    }
#else
    Vector::Vector() :
        _state(std::make_shared<State>(State::MAX_VECTOR_SIZE)),
        _values_ptr(std::make_unique<uint64_t[]>(State::MAX_VECTOR_SIZE)) {
        _values = _values_ptr.get();
        set_default_values();
    }
    Vector::Vector(const int32_t &size) :
        _state(std::make_shared<State>(size)), _values_ptr(std::make_unique<uint64_t[]>(State::MAX_VECTOR_SIZE)) {
        _values = _values_ptr.get();
        set_default_values();
    }
    Vector::Vector(const std::shared_ptr<State> &state) :
        _state(state), _values_ptr(std::make_unique<uint64_t[]>(State::MAX_VECTOR_SIZE)) {
        _values = _values_ptr.get();
        set_default_values();
    }

#endif
    void Vector::set_default_values() const { memset(_values, 0, State::MAX_VECTOR_SIZE * sizeof(uint64_t)); }

    void Vector::allocate_filter() const { _state->allocate_filter(); }

    void Vector::allocate_rle() const { _state->allocate_rle(); }

    void Vector::allocate_selection_bitmask() const { _state->allocate_selection_bitmask(); }

    void Vector::print_debug_info(std::ofstream &logfile) const {
        logfile << "VECTOR DEBUG INFO BEGINS:\n";
        _state->print_debug_info(logfile);

        logfile << "VECTOR DATA VALUES:\n";

        for (int32_t idx = 0; idx < _state->_state_info._size; idx++) {
            logfile << _values[idx] << "\n";
        }

        logfile << "VECTOR DEBUG INFO ENDS:\n";
    }

} // namespace VFEngine
