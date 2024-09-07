#ifndef VFENGINE_STATE_HH
#define VFENGINE_STATE_HH

#include <cstdint>
#include <memory>

namespace VFEngine {
    class State {
    public:
        State() = delete;

        explicit State(const int32_t &size);

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
