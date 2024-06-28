#ifndef SAMPLE_DB_STATE_HH
#define SAMPLE_DB_STATE_HH

#include <stdint.h>

namespace SampleDB
{
    class State
    {
    public:
        State() = delete;
        State(const size_t &);
        uint32_t get_pos() const;
        int32_t get_size() const;
        void update_pos();
        void update_size();
        void print_debug_info() const;

    public:
        static constexpr size_t MAX_VECTOR_SIZE = 1024;

    private:
        int32_t _pos;
        int32_t _size;
    };
}

#endif