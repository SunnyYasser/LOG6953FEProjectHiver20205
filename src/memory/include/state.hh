#ifndef SAMPLE_DB_STATE_HH
#define SAMPLE_DB_STATE_HH

#include <cstdint>
#include <memory>

namespace SampleDB {
    class State {
    public:
        State() = delete;

        explicit State(const int32_t &);

        [[nodiscard]] int32_t get_pos() const;

        [[nodiscard]] int32_t get_size() const;

        void update_pos();

        void update_size();

        void set_pos(const int32_t &);

        void set_size(const int32_t &);

        void print_debug_info() const;

        static constexpr size_t MAX_VECTOR_SIZE = 1024;

    private:
        int32_t _pos;
        int32_t _size;
    };
} // namespace SampleDB
#endif
