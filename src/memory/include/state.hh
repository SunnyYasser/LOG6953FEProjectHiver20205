#ifndef VFENGINE_STATE_HH
#define VFENGINE_STATE_HH

#include <cstdint>
#include <fstream>
#include <memory>
#include "bitmask.hh"

namespace VFEngine {
    struct StateInfo {
        explicit StateInfo(const int32_t &size) : _pos(-1), _size(size) {}
        int32_t _pos;
        int32_t _size;
    };

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
    class State {
    public:
        State() = delete;
        explicit State(const int32_t &size);
        void print_debug_info(std::ofstream &logfile) const;
        static constexpr int32_t MAX_VECTOR_SIZE = 1024;
        void allocate_rle();
        void allocate_selection_bitmask();
        StateInfo _state_info;
        uint32_t *_rle;
        BitMask<MAX_VECTOR_SIZE> *_selection_mask;
    };
#else
    class State {
    public:
        State() = delete;
        explicit State(const int32_t &size);
        void print_debug_info(std::ofstream &logfile) const;
        static constexpr int32_t MAX_VECTOR_SIZE = 1024;
        void allocate_rle();
        void allocate_selection_bitmask();
        StateInfo _state_info;
        std::unique_ptr<uint32_t[]> _rle;
        BitMask<MAX_VECTOR_SIZE> *_selection_mask;

    private:
        std::unique_ptr<BitMask<MAX_VECTOR_SIZE>> _selection_mask_uptr;
    };
#endif
} // namespace VFEngine
#endif
