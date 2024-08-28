#ifndef SAMPLE_DB_VECTOR_HH
#define SAMPLE_DB_VECTOR_HH

#include <memory>
#include <cstdint>
#include <vector>
#include "state.hh"

namespace SampleDB {
    class Vector {
    public:
        Vector();

        explicit Vector(const int32_t &);

        explicit Vector(const std::vector<int32_t> &);

    public:
        bool push_data(const int32_t);

        bool update_data(const int32_t, const int32_t);

        void increment_pos() const;

        void increment_size() const;

    public:
        [[nodiscard]] int32_t get_data(const int32_t) const;

        [[nodiscard]] std::vector<int32_t> get_data_vector() const;

    public:
        void print_debug_info() const;

    public:
        [[nodiscard]] int32_t get_pos() const;

        [[nodiscard]] int32_t get_size() const;

        [[nodiscard]] std::shared_ptr<State> get_state() const;

    private:
        std::vector<int32_t> _vector;
        std::shared_ptr<State> _state;
    };
}

#endif
