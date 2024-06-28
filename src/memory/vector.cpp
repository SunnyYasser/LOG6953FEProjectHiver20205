#include <cassert>
#include <iostream>
#include <algorithm>
#include "include/vector.hh"
namespace SampleDB
{
    Vector::Vector() : _vector(std::vector<int32_t>(State::MAX_VECTOR_SIZE, 0)), _state({State::MAX_VECTOR_SIZE})
    {
    }

    Vector::Vector(const std::vector<int32_t> &vector) : _vector(vector), _state({vector.size()})
    {
    }

    Vector::Vector(const size_t &size) : _vector(std::vector<int32_t>(std::min(State::MAX_VECTOR_SIZE, size), 0)), _state(std::make_shared<State>(std::min(State::MAX_VECTOR_SIZE, size)))
    {
    }

    inline int32_t Vector::get_data(const uint32_t idx) const
    {
        assert(idx < _state.get_size());
        return _vector[idx];
    }

    inline bool Vector::update_data(const uint32_t idx, const int32_t value)
    {
        assert(idx < _state.get_size());
        _vector[idx] = value;

        return true;
    }

    inline bool Vector::push_data(const int32_t value)
    {
        assert(_state.get_size() < State::MAX_VECTOR_SIZE);
        _vector[_state.get_size()] = value;
        increment_size();

        return true;
    }

    inline int32_t Vector::get_size() const
    {
        return _state.get_size();
    }

    inline const std::vector<int32_t> Vector::get_data_vector() const
    {
        return _vector;
    }

    void Vector::print_debug_info() const
    {
        std::cout << "VECTOR DEBUG INFO BEGINS:\n";
        _state.print_debug_info();

        std::cout << "VECTOR DATA VALUES:\n";

        for (auto &v : _vector)
        {
            std::cout << v << "\n";
        }

        std::cout << "VECTOR DEBUG INFO ENDS:\n";
    }

    inline int32_t Vector::get_pos() const
    {
        return _state.get_pos();
    }

    inline void Vector::increment_pos()
    {
        _state.update_pos();
    }

    inline void Vector::increment_size()
    {
        _state.update_size();
    }

    inline State Vector::get_state() const
    {
        return _state;
    }
}