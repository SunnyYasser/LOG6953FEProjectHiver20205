#ifndef SAMPLE_DB_VECTOR_HH
#define SAMPLE_DB_VECTOR_HH

#include <cstdint>
#include <memory>
#include <vector>
#include "state.hh"

namespace SampleDB {
    class Vector {
    public:
        Vector();

        explicit Vector(const int32_t &);

        explicit Vector(const std::vector<int32_t> &);

        Vector(const Vector &) = default;
        Vector &operator=(const Vector &) = default;

    public:
        bool push_data(const int32_t &);

        bool update_data(const int32_t &, const int32_t &);

        void increment_pos() const;

        void increment_size() const;

        void set_data_vector(const std::vector<int32_t> &);

        void set_pos(const int32_t &value) const;

        void set_size(const int32_t &) const;

        void set_state(const std::shared_ptr<State> &);

    public:
        [[nodiscard]] int32_t get_data(const int32_t &) const;

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
} // namespace SampleDB

#endif
