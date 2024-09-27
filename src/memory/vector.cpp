#include "include/vector.hh"
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

    Vector::Vector(const std::shared_ptr<State> &state) :
        _state(state), _values_uptr(std::make_unique<uint64_t[]>(State::MAX_VECTOR_SIZE)) {
        _values = _values_uptr.get();
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
