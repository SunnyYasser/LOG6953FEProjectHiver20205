#ifndef VFENGINE_STATE_HH
#define VFENGINE_STATE_HH

#include <cstdint>
#include <fstream>
#include <memory>
#include "bitmask.hh"

namespace VFEngine {
    struct StateInfo {
        explicit StateInfo(const int32_t &size) : _pos(-1), _size(size), _curr_start_pos(0) {}
        int32_t _pos;
        int32_t _size;
        int32_t _curr_start_pos;
    };

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
#ifdef MEMSET_TO_SET_VECTOR_SLICE
    class State {
    public:
        State() = delete;
        explicit State(const int32_t &size);
        void print_debug_info(std::ofstream &logfile) const;
        static constexpr int32_t MAX_VECTOR_SIZE = 1024;
        void allocate_filter();
        void allocate_rle();
        void allocate_selection_bitmask();
        StateInfo _state_info;
        int32_t _rle_size;
        int32_t _filter_size;
        uint32_t *_rle, *_filter_list;
        BitMask<MAX_VECTOR_SIZE> *_selection_mask;
    };
#else
    class State {
    public:
        State() = delete;
        explicit State(const int32_t &size);
        void print_debug_info(std::ofstream &logfile) const;
        static constexpr int32_t MAX_VECTOR_SIZE = 1024;
        void allocate_filter();
        void allocate_rle();
        void allocate_selection_bitmask();
        StateInfo _state_info;
        int32_t _rle_size;
        int32_t _filter_size;
        int32_t _rle_start_pos;
        uint32_t *_rle, *_filter_list;
        BitMask<MAX_VECTOR_SIZE> *_selection_mask;
    };
#endif

#else
#ifdef MEMSET_TO_SET_VECTOR_SLICE
    class State {
    public:
        State() = delete;
        explicit State(const int32_t &size);
        void print_debug_info(std::ofstream &logfile) const;
        static constexpr int32_t MAX_VECTOR_SIZE = 1024;
        void allocate_filter();
        void allocate_rle();
        void allocate_selection_bitmask();
        StateInfo _state_info;
        int32_t _rle_size;
        int32_t _filter_size;
        std::unique_ptr<uint32_t[]> _rle, _filter_list;
        std::unique_ptr<BitMask<MAX_VECTOR_SIZE>> _selection_mask_uptr;
        BitMask<MAX_VECTOR_SIZE> *_selection_mask;
    };
#else
    class State {
    public:
        State() = delete;
        explicit State(const int32_t &size);
        void print_debug_info(std::ofstream &logfile) const;
        static constexpr int32_t MAX_VECTOR_SIZE = 1024;
        void allocate_filter();
        void allocate_rle();
        void allocate_selection_bitmask();
        StateInfo _state_info;
        int32_t _rle_size;
        int32_t _filter_size;
        int32_t _rle_start_pos;
        std::unique_ptr<uint32_t[]> _rle, _filter_list;
        std::unique_ptr<BitMask<MAX_VECTOR_SIZE>> _selection_mask_uptr;
        BitMask<MAX_VECTOR_SIZE> *_selection_mask;
    };
#endif
#endif
} // namespace VFEngine
#endif
