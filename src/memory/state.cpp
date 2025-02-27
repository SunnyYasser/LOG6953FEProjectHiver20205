#include "include/state.hh"
#include <cstring>
#include "../graph/include/arena_allocator.hh"

namespace VFEngine {

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
    State::State(const int32_t &size) : _state_info(size), _rle(nullptr), _selection_mask(nullptr) {}

    void State::allocate_rle() {
        if (_rle) {
            return;
        }
        _rle = static_cast<uint32_t *>(
                ArenaAllocator::getInstance().allocate((MAX_VECTOR_SIZE + 1) * sizeof(uint32_t)));
        std::memset(_rle, 0, (MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
        // Initialize RLE[0] to 0
        _rle[0] = 0;
    }

    void State::allocate_selection_bitmask() {
        if (_selection_mask) {
            return;
        }
        void *memory = ArenaAllocator::getInstance().allocate(sizeof(BitMask<MAX_VECTOR_SIZE>));
        _selection_mask = new (memory) BitMask<MAX_VECTOR_SIZE>();
    }

    void State::print_debug_info(std::ofstream &logfile) const {
        logfile << "[STATE pos]: " << _state_info._pos << std::endl;
        logfile << "[STATE size]: " << _state_info._size << std::endl;

        if (_rle) {
            logfile << "[STATE rle]: " << std::endl;
            // Print all RLE values from 0 to MAX_VECTOR_SIZE
            for (int32_t i = 0; i <= MAX_VECTOR_SIZE; i++) {
                logfile << _rle[i] << " ";
                if ((i + 1) % 20 == 0)
                    logfile << std::endl; // Line break every 20 values for readability
            }
            logfile << std::endl;
        }

        if (_selection_mask) {
            logfile << "[STATE selection_mask]:" << std::endl;
            logfile << "start_pos: " << GET_START_POS(*_selection_mask) << std::endl;
            logfile << "end_pos: " << GET_END_POS(*_selection_mask) << std::endl;

#ifdef BIT_ARRAY_AS_FILTER
            // Print bit array implementation (one byte per bit)
            logfile << "bits (array implementation): " << std::endl;
            for (int32_t i = 0; i < MAX_VECTOR_SIZE; i++) {
                if (i > 0 && i % 64 == 0)
                    logfile << std::endl;
                logfile << (TEST_BIT(*_selection_mask, i) ? "1" : "0");
                if ((i + 1) % 8 == 0)
                    logfile << " "; // Space every 8 bits for readability
            }
#else
            // Print packed bit implementation (bits packed in uint64_t)
            logfile << "bits (packed implementation): " << std::endl;
            constexpr std::size_t REQUIRED_UINT64 = (MAX_VECTOR_SIZE + 63) / 64;
            for (std::size_t block = 0; block < REQUIRED_UINT64; block++) {
                logfile << "Block " << block << ": ";
                for (std::size_t bit = 0; bit < 64 && (block * 64 + bit) < MAX_VECTOR_SIZE; bit++) {
                    int32_t index = static_cast<int32_t>(block * 64 + bit);
                    logfile << (TEST_BIT(*_selection_mask, index) ? "1" : "0");
                    if ((bit + 1) % 8 == 0)
                        logfile << " "; // Space every 8 bits
                }
                logfile << std::endl;
            }
#endif
            logfile << std::endl;
        }

        logfile << std::endl;
    }

#else
    State::State(const int32_t &size) : _state_info(size), _selection_mask_uptr(nullptr), _selection_mask(nullptr) {}

    void State::allocate_rle() {
        if (_rle) {
            return;
        }
        _rle = std::make_unique<uint32_t[]>(State::MAX_VECTOR_SIZE + 1);
        std::memset(_rle.get(), 0, (MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
        // Initialize RLE[0] to 0
        _rle[0] = 0;
    }

    void State::allocate_selection_bitmask() {
        if (_selection_mask_uptr) {
            return;
        }
        _selection_mask_uptr = std::make_unique<BitMask<MAX_VECTOR_SIZE>>();
        _selection_mask = _selection_mask_uptr.get();
    }

    void State::print_debug_info(std::ofstream &logfile) const {
        logfile << "[STATE pos]: " << _state_info._pos << std::endl;
        logfile << "[STATE size]: " << _state_info._size << std::endl;

        if (_rle) {
            logfile << "[STATE rle]: " << std::endl;
            // Print all RLE values from 0 to MAX_VECTOR_SIZE
            for (int32_t i = 0; i <= MAX_VECTOR_SIZE; i++) {
                logfile << _rle[i] << " ";
                if ((i + 1) % 20 == 0)
                    logfile << std::endl; // Line break every 20 values for readability
            }
            logfile << std::endl;
        }

        if (_selection_mask) {
            logfile << "[STATE selection_mask]:" << std::endl;
            logfile << "start_pos: " << GET_START_POS(*_selection_mask) << std::endl;
            logfile << "end_pos: " << GET_END_POS(*_selection_mask) << std::endl;

#ifdef BIT_ARRAY_AS_FILTER
            // Print bit array implementation (one byte per bit)
            logfile << "bits (array implementation): " << std::endl;
            for (int32_t i = 0; i < MAX_VECTOR_SIZE; i++) {
                if (i > 0 && i % 64 == 0)
                    logfile << std::endl;
                logfile << (TEST_BIT(*_selection_mask, i) ? "1" : "0");
                if ((i + 1) % 8 == 0)
                    logfile << " "; // Space every 8 bits for readability
            }
#else
            // Print packed bit implementation (bits packed in uint64_t)
            logfile << "bits (packed implementation): " << std::endl;
            constexpr std::size_t REQUIRED_UINT64 = (MAX_VECTOR_SIZE + 63) / 64;
            for (std::size_t block = 0; block < REQUIRED_UINT64; block++) {
                logfile << "Block " << block << ": ";
                for (std::size_t bit = 0; bit < 64 && (block * 64 + bit) < MAX_VECTOR_SIZE; bit++) {
                    int32_t index = static_cast<int32_t>(block * 64 + bit);
                    logfile << (TEST_BIT(*_selection_mask, index) ? "1" : "0");
                    if ((bit + 1) % 8 == 0)
                        logfile << " "; // Space every 8 bits
                }
                logfile << std::endl;
            }
#endif
            logfile << std::endl;
        }

        logfile << std::endl;
    }
#endif

} // namespace VFEngine
