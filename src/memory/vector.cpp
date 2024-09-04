#include <cassert>
#include <iostream>
#include <algorithm>
#include "include/vector.hh"

namespace SampleDB {
    Vector::Vector() : _vector(std::vector<int32_t>(State::MAX_VECTOR_SIZE, 0)),
                       _state(std::make_shared<State>(State::MAX_VECTOR_SIZE)) {
    }

    Vector::Vector(const std::vector<int32_t> &vector) : _vector(vector),
                                                         _state(std::make_shared<State>(
                                                             std::min(vector.size(), State::MAX_VECTOR_SIZE))) {
    }

    Vector::Vector(const int32_t &size) : _vector(std::vector<int32_t>(std::min((int32_t) State::MAX_VECTOR_SIZE, size),
                                                                       0)),
                                          _state(std::make_shared<State>(
                                              std::min(size, (int32_t) State::MAX_VECTOR_SIZE))) {
    }


    int32_t Vector::get_data(const int32_t& idx) const {
        assert(idx < _state->get_size());
        return _vector[idx];
    }

    bool Vector::update_data(const int32_t& idx, const int32_t& value) {
        assert(idx < _state->get_size());
        _vector[idx] = value;

        return true;
    }

    bool Vector::push_data(const int32_t& value) {
        assert(_state->get_size() < State::MAX_VECTOR_SIZE);
        _vector[_state->get_size()] = value;
        increment_size();

        return true;
    }

    int32_t Vector::get_size() const {
        return _state->get_size();
    }

    std::vector<int32_t> Vector::get_data_vector() const {
        return _vector;
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

    int32_t Vector::get_pos() const {
        return _state->get_pos();
    }

    void Vector::increment_pos() const {
        _state->update_pos();
    }

    void Vector::increment_size() const {
        _state->update_size();
    }

    std::shared_ptr<State> Vector::get_state() const {
        return _state;
    }

    void Vector::set_data_vector(const std::vector<int32_t> & data) {
        _vector = std::vector (begin(data), end(data));
    }

    void Vector::set_pos(const int32_t& value) const {
        _state->set_pos(value);
    }

    void Vector::set_size(const int32_t& value) const {
        _state->set_size(value);
    }

    void Vector::set_state (const std::shared_ptr<State>& state) {
        _state = state;
    }

}
