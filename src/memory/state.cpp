#include "include/state.hh"
#include <cstring>
#include "../graph/include/arena_allocator.hh"

namespace VFEngine {

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
#ifdef MEMSET_TO_SET_VECTOR_SLICE
    State::State(const int32_t &size) :
        _state_info(size), _rle_size(-1), _filter_size(-1), _filter_list(nullptr), _rle(nullptr),
        _selection_mask(nullptr) {}

    void State::allocate_filter() {
        if (_filter_list) {
            return;
        }
        _filter_list =
                static_cast<uint32_t *>(ArenaAllocator::getInstance().allocate(MAX_VECTOR_SIZE * sizeof(uint32_t)));
        _filter_size = 0;
    }

    void State::allocate_rle() {
        if (_rle) {
            return;
        }
        _rle = static_cast<uint32_t *>(
                ArenaAllocator::getInstance().allocate((MAX_VECTOR_SIZE + 1) * sizeof(uint32_t)));
        std::memset(_rle, 0, (MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
        _rle_size = 1;
    }

    void State::allocate_selection_bitmask() {
        if (_selection_mask) {
            return;
        }
        _selection_mask = static_cast<BitMask<MAX_VECTOR_SIZE> *>(
                ArenaAllocator::getInstance().allocate(sizeof(BitMask<MAX_VECTOR_SIZE>)));
        SET_ALL_BITS(*_selection_mask);
    }

    void State::print_debug_info(std::ofstream &logfile) const {
        logfile << "[STATE pos]: " << _state_info._pos << std::endl;
        logfile << "[STATE size]: " << _state_info._size << std::endl;
        logfile << "[STATE parent_start_pos]: " << _state_info._curr_start_pos << std::endl;

        if (_filter_list) {
            logfile << "[STATE filter]: " << std::endl;
            for (int32_t i = 0; i < _filter_size; i++) {
                logfile << _filter_list[i] << " ";
            }
            logfile << std::endl;
        }

        if (_rle) {
            logfile << "[STATE rle]: " << std::endl;
            for (int32_t i = 0; i < _rle_size; i++) {
                logfile << _rle[i] << " ";
            }
            logfile << std::endl;
        }

        logfile << std::endl;
    }
#else
    State::State(const int32_t &size) :
        _state_info(size), _rle_size(-1), _filter_size(-1), _rle_start_pos(0), _filter_list(nullptr), _rle(nullptr),
        _selection_mask(nullptr) {}

    void State::allocate_filter() {
        if (_filter_list) {
            return;
        }
        _filter_list =
                static_cast<uint32_t *>(ArenaAllocator::getInstance().allocate(MAX_VECTOR_SIZE * sizeof(uint32_t)));
        _filter_size = 0;
    }

    void State::allocate_rle() {
        if (_rle) {
            return;
        }
        _rle = static_cast<uint32_t *>(
                ArenaAllocator::getInstance().allocate((MAX_VECTOR_SIZE + 1) * sizeof(uint32_t)));
        std::memset(_rle, 0, (MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
        _rle_size = 1;
    }

    void State::allocate_selection_bitmask() {
        if (_selection_mask) {
            return;
        }
        _selection_mask = static_cast<BitMask<MAX_VECTOR_SIZE> *>(
                ArenaAllocator::getInstance().allocate(sizeof(BitMask<MAX_VECTOR_SIZE>)));
        SET_ALL_BITS(*_selection_mask);
    }

    void State::print_debug_info(std::ofstream &logfile) const {
        logfile << "[STATE pos]: " << _state_info._pos << std::endl;
        logfile << "[STATE size]: " << _state_info._size << std::endl;
        logfile << "[STATE parent_start_pos]: " << _state_info._curr_start_pos << std::endl;

        if (_filter_list) {
            logfile << "[STATE filter]: " << std::endl;
            for (int32_t i = 0; i < _filter_size; i++) {
                logfile << _filter_list[i] << " ";
            }
            logfile << std::endl;
        }

        if (_rle) {
            logfile << "[STATE rle]: " << std::endl;
            for (int32_t i = 0; i < _rle_size; i++) {
                logfile << _rle[i] << " ";
            }
            logfile << std::endl;
            logfile << "[STATE rle_start_pos]: " << _rle_start_pos << std::endl;
        }

        logfile << std::endl;
    }
#endif

#else
#ifdef MEMSET_TO_SET_VECTOR_SLICE
    State::State(const int32_t &size) :
        _state_info(size), _rle_size(-1), _filter_size(-1), _selection_mask_uptr(nullptr), _selection_mask(nullptr) {}

    void State::allocate_filter() {
        if (_filter_list) {
            return;
        }
        _filter_list = std::make_unique<uint32_t[]>(State::MAX_VECTOR_SIZE);
        _filter_size = 0;
    }

    void State::allocate_rle() {
        if (_rle) {
            return;
        }
        _rle = std::make_unique<uint32_t[]>(State::MAX_VECTOR_SIZE + 1);
        std::memset(_rle.get(), 0, (MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
        _rle_size = 1;
    }

    void State::allocate_selection_bitmask() {
        if (_selection_mask_uptr) {
            return;
        }
        _selection_mask_uptr = std::make_unique<BitMask<MAX_VECTOR_SIZE>>();
        _selection_mask = _selection_mask_uptr.get();
        SET_ALL_BITS(*_selection_mask);
    }

    void State::print_debug_info(std::ofstream &logfile) const {
        logfile << "[STATE pos]: " << _state_info._pos << std::endl;
        logfile << "[STATE size]: " << _state_info._size << std::endl;
        logfile << "[STATE parent_start_pos]: " << _state_info._curr_start_pos << std::endl;

        if (_filter_list) {
            logfile << "[STATE filter]: " << std::endl;
            for (int32_t i = 0; i < _filter_size; i++) {
                logfile << _filter_list[i] << " ";
            }
            logfile << std::endl;
        }

        if (_rle) {
            logfile << "[STATE rle]: " << std::endl;
            for (int32_t i = 0; i < _rle_size; i++) {
                logfile << _rle[i] << " ";
            }
            logfile << std::endl;
        }

        logfile << std::endl;
    }
#else
    State::State(const int32_t &size) :
        _state_info(size), _rle_size(-1), _filter_size(-1), _rle_start_pos(0), _selection_mask_uptr(nullptr),
        _selection_mask(nullptr) {}

    void State::allocate_filter() {
        if (_filter_list) {
            return;
        }
        _filter_list = std::make_unique<uint32_t[]>(State::MAX_VECTOR_SIZE);
        _filter_size = 0;
    }

    void State::allocate_rle() {
        if (_rle) {
            return;
        }
        _rle = std::make_unique<uint32_t[]>(State::MAX_VECTOR_SIZE + 1);
        std::memset(_rle.get(), 0, (MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
        _rle_size = 1;
    }

    void State::allocate_selection_bitmask() {
        if (_selection_mask_uptr) {
            return;
        }
        _selection_mask_uptr = std::make_unique<BitMask<MAX_VECTOR_SIZE>>();
        _selection_mask = _selection_mask_uptr.get();
        SET_ALL_BITS(*_selection_mask);
    }

    void State::print_debug_info(std::ofstream &logfile) const {
        logfile << "[STATE pos]: " << _state_info._pos << std::endl;
        logfile << "[STATE size]: " << _state_info._size << std::endl;
        logfile << "[STATE parent_start_pos]: " << _state_info._curr_start_pos << std::endl;

        if (_filter_list) {
            logfile << "[STATE filter]: " << std::endl;
            for (int32_t i = 0; i < _filter_size; i++) {
                logfile << _filter_list[i] << " ";
            }
            logfile << std::endl;
        }

        if (_rle) {
            logfile << "[STATE rle]: " << std::endl;
            for (int32_t i = 0; i < _rle_size; i++) {
                logfile << _rle[i] << " ";
            }
            logfile << std::endl;
            logfile << "[STATE rle_start_pos]: " << _rle_start_pos << std::endl;
        }

        logfile << std::endl;
    }
#endif
#endif

} // namespace VFEngine
