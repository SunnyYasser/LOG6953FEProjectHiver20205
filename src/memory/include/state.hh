#ifndef VFENGINE_STATE_HH
#define VFENGINE_STATE_HH

#include <cstdint>
#include <fstream>
#include <memory>

namespace VFEngine {
    struct StateInfo {
        explicit StateInfo(const int32_t &size) : _pos(-1), _size(size), _curr_start_pos(0) {}
        int32_t _pos;
        int32_t _size;
        int32_t _curr_start_pos;
    };

    class State {
    public:
        State() = delete;
        explicit State(const int32_t &size);
        void print_debug_info(std::ofstream &logfile) const;
        static constexpr int32_t MAX_VECTOR_SIZE = 1024;
        void allocate_filter();
        void allocate_rle();
        StateInfo _state_info;
        int32_t _rle_size;
        int32_t _filter_size;
        std::unique_ptr<uint32_t[]> _rle, _filter_list;
    };
} // namespace VFEngine
#endif
