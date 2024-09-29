#include "include/vector.hh"
#include <iostream>

namespace VFEngine {
    Vector::Vector() :
        _state(std::make_shared<State>(State::MAX_VECTOR_SIZE)),
        _data_uptr(static_cast<uint8_t *>(
                           std::aligned_alloc(alignof(uint64_t), (State::MAX_VECTOR_SIZE * sizeof(uint64_t)) +
                                                                         (State::MAX_VECTOR_SIZE * sizeof(bool)))),
                   &std::free) {
        _values = reinterpret_cast<uint64_t *>(_data_uptr.get());
        _filter = reinterpret_cast<bool *>(_data_uptr.get() + (State::MAX_VECTOR_SIZE * sizeof(uint64_t)));
        set_default_values();
    }

    Vector::Vector(const int32_t &size) :
        _state(std::make_shared<State>(size)),
        _data_uptr(static_cast<uint8_t *>(
                           std::aligned_alloc(alignof(uint64_t), (State::MAX_VECTOR_SIZE * sizeof(uint64_t)) +
                                                                         (State::MAX_VECTOR_SIZE * sizeof(bool)))),
                   &std::free) {
        _values = reinterpret_cast<uint64_t *>(_data_uptr.get());
        _filter = reinterpret_cast<bool *>(_data_uptr.get() + (State::MAX_VECTOR_SIZE * sizeof(uint64_t)));
        set_default_values();
    }

    Vector::Vector(const std::shared_ptr<State> &state) :
        _state(state), _data_uptr(static_cast<uint8_t *>(std::aligned_alloc(
                                          alignof(uint64_t), (State::MAX_VECTOR_SIZE * sizeof(uint64_t)) +
                                                                     (State::MAX_VECTOR_SIZE * sizeof(bool)))),
                                  &std::free) {
        _values = reinterpret_cast<uint64_t *>(_data_uptr.get());
        _filter = reinterpret_cast<bool *>(_data_uptr.get() + (State::MAX_VECTOR_SIZE * sizeof(uint64_t)));
        set_default_values();
    }

    void Vector::set_default_values() {
        std::fill_n(_values, State::MAX_VECTOR_SIZE, 42); // just a default value
        std::fill_n(_filter, State::MAX_VECTOR_SIZE, true); // assume every value is valid
    }

    void Vector::print_debug_info(std::ofstream& logfile) const{
        logfile << "VECTOR DEBUG INFO BEGINS:\n";
        _state->print_debug_info(logfile);

        logfile << "VECTOR DATA VALUES:\n";

        for (int32_t idx = 0; idx < _state->_size; idx++) {
            logfile << _values[idx] << "\n";
        }

        logfile << "VECTOR DEBUG INFO ENDS:\n";
    }

} // namespace VFEngine
