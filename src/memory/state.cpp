#include <iostream>
#include "include/state.hh"
namespace SampleDB
{
    inline State::State(const size_t & size) : _pos(-1), _size(size) {}

    inline uint32_t State::get_pos() const
    {
        return _pos;
    }

    inline int32_t State::get_size() const
    {
        return _size;
    }

    inline void State::update_pos()
    {
        _pos++;
    }

    inline void State::update_size()
    {
        _size++;
    }

    void State::print_debug_info() const
    {
        std::cout << "[STATE pos]: " << _pos << std::endl;
        std::cout << "[STATE size]: " << _size << std::endl;
    }

}