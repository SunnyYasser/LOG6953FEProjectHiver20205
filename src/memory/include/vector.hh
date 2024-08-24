#ifndef SAMPLE_DB_VECTOR_HH
#define SAMPLE_DB_VECTOR_HH

#include <stdint.h>
#include <vector>
#include "state.hh"

namespace SampleDB
{
    class Vector
    {
    public:
        Vector();
        Vector(const size_t &);
        Vector(const std::vector<int32_t> &);

    public:
        bool push_data(const int32_t);
        bool update_data(const uint32_t, const int32_t);
        void increment_pos();
        void increment_size();

    public:
        int32_t get_data(const uint32_t) const;
        const std::vector<int32_t> get_data_vector() const;

    public:
        void print_debug_info() const;

    public:
        int32_t get_pos() const;
        int32_t get_size() const;
        State get_state();

    private:
        std::vector<int32_t> _vector;
        State _state;
    };
}

#endif