#include <iostream>
#include "include/state.hh"
namespace SampleDB
{
    State::State(const size_t & size) : _pos(-1), _size(size) {}

    uint32_t State::get_pos() const
    {
        return _pos;
    }

    int32_t State::get_size() const
    {
        return _size;
    }

    void State::update_pos()
    {
        _pos++;
    }

    void State::update_size()
    {
        _size++;
    }

    void State::print_debug_info() const
    {
        std::cout << "[STATE pos]: " << _pos << std::endl;
        std::cout << "[STATE size]: " << _size << std::endl;
    }

}