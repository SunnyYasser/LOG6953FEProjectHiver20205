#include "bitmask.hh"
#include <cstring>
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
#include "../graph/include/arena_allocator.hh"
#endif
namespace VFEngine {

#ifdef BIT_ARRAY_AS_FILTER
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
    // Arena allocator implementation
    template<std::size_t N>
    BitMask<N>::BitMask() : start_pos(0), end_pos(N - 1) {
        bits = static_cast<uint8_t *>(ArenaAllocator::getInstance().allocate(N * sizeof(uint8_t)));
        setAllBits();
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) : start_pos(other.start_pos), end_pos(other.end_pos) {
        bits = static_cast<uint8_t *>(ArenaAllocator::getInstance().allocate(N * sizeof(uint8_t)));
        copyFrom(other);
    }

    template<std::size_t N>
    BitMask<N>::~BitMask() {
        // No explicit delete needed when using ArenaAllocator
        // Memory is freed when the allocator is destroyed
    }
#else
    // Standard allocator implementation with unique_ptr
    template<std::size_t N>
    BitMask<N>::BitMask() : start_pos(0), end_pos(N - 1) {
        bits_uptr = std::make_unique<uint8_t[]>(N);
        bits = bits_uptr.get();
        setAllBits();
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) : start_pos(other.start_pos), end_pos(other.end_pos) {
        bits_uptr = std::make_unique<uint8_t[]>(N);
        bits = bits_uptr.get();
        copyFrom(other);
    }

#endif
    template<std::size_t N>
    BitMask<N> &BitMask<N>::operator=(const BitMask &other) {
        if (this != &other) {
            copyFrom(other);
            start_pos = other.start_pos;
            end_pos = other.end_pos;
        }
        return *this;
    }

    template<std::size_t N>
    void BitMask<N>::setBit(std::size_t index) {
        bits[index] = 1;
        if (index < start_pos)
            start_pos = index;
        if (index > end_pos)
            end_pos = index;
    }

    template<std::size_t N>
    void BitMask<N>::clearBit(std::size_t index) {
        bits[index] = 0;
        // If we cleared either boundary bit, we need to recalculate positions
        if (index == start_pos || index == end_pos) {
            updatePositions();
        }
    }

    template<std::size_t N>
    bool BitMask<N>::testBit(std::size_t index) const {
        return bits[index] != 0;
    }

    template<std::size_t N>
    void BitMask<N>::toggleBit(std::size_t index) {
        bits[index] = !bits[index];
        if (bits[index]) { // If bit is now set
            if (index < start_pos)
                start_pos = index;
            if (index > end_pos)
                end_pos = index;
        } else if (index == start_pos || index == end_pos) { // If bit is now cleared and was a boundary
            updatePositions();
        }
    }

    template<std::size_t N>
    void BitMask<N>::clearAllBits() {
        for (auto i = 0; i < N; i++)
            bits[i] = 0;
        start_pos = N; // No bits set, use max value
        end_pos = 0; // No bits set, use min value
    }

    template<std::size_t N>
    void BitMask<N>::setAllBits() {
        for (auto i = 0; i < N; i++)
            bits[i] = 1;
        start_pos = 0;
        end_pos = N - 1;
    }

    template<std::size_t N>
    void BitMask<N>::andWith(const BitMask &other) {
        for (std::size_t i = 0; i < N; ++i) {
            bits[i] = bits[i] && other.bits[i];
        }
        updatePositions();
    }

    template<std::size_t N>
    void BitMask<N>::copyFrom(const BitMask &other) {
        for (auto i = 0; i < N; i++)
            bits[i] = other.bits[i];
        start_pos = other.start_pos;
        end_pos = other.end_pos;
    }

    template<std::size_t N>
    void BitMask<N>::updatePositions() {
        // Update start_pos (first set bit)
        start_pos = N; // Default to max if no bits are set
        for (std::size_t i = 0; i < N; ++i) {
            if (bits[i]) {
                start_pos = i;
                break;
            }
        }

        // Update end_pos (last set bit)
        end_pos = 0; // Default to min if no bits are set
        for (std::size_t i = N; i-- > 0;) {
            if (bits[i]) {
                end_pos = i;
                break;
            }
        }
    }

    template<std::size_t N>
    int32_t BitMask<N>::getStartPos() const {
        return start_pos;
    }

    template<std::size_t N>
    int32_t BitMask<N>::getEndPos() const {
        return end_pos;
    }

    template<std::size_t N>
    void BitMask<N>::setStartPos(const int32_t idx_value) {
        start_pos = idx_value;
    }

    template<std::size_t N>
    void BitMask<N>::setEndPos(const int32_t idx_value) {
        end_pos = idx_value;
    }

#else

    template<std::size_t N>
    BitMask<N>::BitMask() : start_pos(N), end_pos(-1) {
        setAllBits();
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) : start_pos(other.start_pos), end_pos(other.end_pos) {
        copyFrom(other);
    }

    template<std::size_t N>
    BitMask<N> &BitMask<N>::operator=(const BitMask &other) {
        if (this != &other) {
            copyFrom(other);
        }
        return *this;
    }

    template<std::size_t N>
    void BitMask<N>::setBit(const std::size_t index) {
        bits[getUint64Index(index)] |= getBitMask(index);
        updatePositionsOnSet(static_cast<int32_t>(index));
    }

    template<std::size_t N>
    void BitMask<N>::clearBit(const std::size_t index) {
        bits[getUint64Index(index)] &= ~getBitMask(index);
        if (index == static_cast<std::size_t>(start_pos) || index == static_cast<std::size_t>(end_pos)) {
            updatePositions();
        }
    }

    template<std::size_t N>
    bool BitMask<N>::testBit(const std::size_t index) const {
        return (bits[getUint64Index(index)] & getBitMask(index)) != 0;
    }

    template<std::size_t N>
    void BitMask<N>::toggleBit(const std::size_t index) {
        bits[getUint64Index(index)] ^= getBitMask(index);
        updatePositions();
    }

    template<std::size_t N>
    void BitMask<N>::clearAllBits() {
        std::memset(bits.data(), 0, sizeof(bits));
        start_pos = N;
        end_pos = -1;
    }

    template<std::size_t N>
    void BitMask<N>::setAllBits() {
        std::memset(bits.data(), 0xFF, sizeof(bits));
        start_pos = 0;
        end_pos = N - 1;
    }

    template<std::size_t N>
    void BitMask<N>::andWith(const BitMask &other) {
        for (std::size_t i = 0; i < REQUIRED_UINT64<N>; ++i) {
            bits[i] &= other.bits[i];
        }
        updatePositions();
    }

    template<std::size_t N>
    void BitMask<N>::copyFrom(const BitMask &other) {
        std::memcpy(bits.data(), other.bits.data(), sizeof(bits));
        start_pos = other.start_pos;
        end_pos = other.end_pos;
    }

    template<std::size_t N>
    void BitMask<N>::updatePositions() {
        start_pos = N;
        end_pos = -1;

        for (std::size_t block = 0; block < REQUIRED_UINT64<N>; ++block) {
            if (bits[block]) {
                const uint64_t val = bits[block];
                for (std::size_t bit = 0; bit < BITS_PER_UINT64 && (block * BITS_PER_UINT64 + bit) < N; ++bit) {
                    if (val & (1ULL << bit)) {
                        int32_t index = block * BITS_PER_UINT64 + bit;
                        start_pos = std::min(start_pos, index);
                        end_pos = std::max(end_pos, index);
                    }
                }
            }
        }
    }

    template<std::size_t N>
    void BitMask<N>::updatePositionsOnSet(int32_t index) {
        start_pos = std::min(start_pos, index);
        end_pos = std::max(end_pos, index);
    }

    template<std::size_t N>
    int32_t BitMask<N>::getStartPos() const {
        return start_pos;
    }

    template<std::size_t N>
    int32_t BitMask<N>::getEndPos() const {
        return end_pos;
    }

    template<std::size_t N>
    void BitMask<N>::setStartPos(const int32_t idx_value) {
        start_pos = idx_value;
    }

    template<std::size_t N>
    void BitMask<N>::setEndPos(const int32_t idx_value) {
        end_pos = idx_value;
    }


#endif

    template class BitMask<1024>;
    template class BitMask<2048>;

} // namespace VFEngine
