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
        setBitsTillIdx(N - 1);
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) : start_pos(other.start_pos), end_pos(other.end_pos) {
        bits = static_cast<uint8_t *>(ArenaAllocator::getInstance().allocate(N * sizeof(uint8_t)));
        copyFrom(other);
    }

#else
    // Standard allocator implementation with unique_ptr
    template<std::size_t N>
    BitMask<N>::BitMask() : start_pos(0), end_pos(N - 1) {
        bits_uptr = std::make_unique<uint8_t[]>(N);
        bits = bits_uptr.get();
        setBitsTillIdx(N - 1);
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
        }
        return *this;
    }

    template<std::size_t N>
    void BitMask<N>::setBit(const std::size_t index) const {
        bits[index] = 1;
    }

    template<std::size_t N>
    void BitMask<N>::clearBit(const std::size_t index) const {
        bits[index] = 0;
    }

    template<std::size_t N>
    bool BitMask<N>::testBit(const std::size_t index) const {
        return bits[index] != 0;
    }

    template<std::size_t N>
    void BitMask<N>::toggleBit(const std::size_t index) const {
        bits[index] = !bits[index];
    }

    template<std::size_t N>
    void BitMask<N>::setBitsTillIdx(const std::size_t idx) const {
        for (auto i = 0; i <= idx; i++)
            bits[i] = 1;
    }

    template<std::size_t N>
    void BitMask<N>::clearBitsTillIdx(const std::size_t idx) const {
        for (auto i = 0; i <= idx; i++)
            bits[i] = 0;
    }

    template<std::size_t N>
    void BitMask<N>::andWith(const BitMask &other) {
        for (std::size_t i = 0; i < N; ++i) {
            bits[i] = bits[i] && other.bits[i];
        }
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

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
    template<std::size_t N>
    BitMask<N>::BitMask() : start_pos(0), end_pos(N - 1) {
        const std::size_t numBlocks = REQUIRED_UINT64<N>;
        bits = static_cast<uint64_t *>(ArenaAllocator::getInstance().allocate(numBlocks * sizeof(uint64_t)));
        setBitsTillIdx(N - 1);
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) : start_pos(other.start_pos), end_pos(other.end_pos) {
        const std::size_t numBlocks = REQUIRED_UINT64<N>;
        bits = static_cast<uint64_t *>(ArenaAllocator::getInstance().allocate(numBlocks * sizeof(uint64_t)));
        copyFrom(other);
    }

    // No explicit destructor needed when using ArenaAllocator
    // Memory is freed when the allocator is destroyed
#else
    template<std::size_t N>
    BitMask<N>::BitMask() : start_pos(0), end_pos(N - 1) {
        const std::size_t numBlocks = REQUIRED_UINT64<N>;
        bits_uptr = std::make_unique<uint64_t[]>(numBlocks);
        bits = bits_uptr.get();
        setBitsTillIdx(N - 1);
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) : start_pos(other.start_pos), end_pos(other.end_pos) {
        const std::size_t numBlocks = REQUIRED_UINT64<N>;
        bits_uptr = std::make_unique<uint64_t[]>(numBlocks);
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
    void BitMask<N>::setBit(const std::size_t index) const {
        bits[getUint64Index(index)] |= getBitMask(index);
    }

    template<std::size_t N>
    void BitMask<N>::clearBit(const std::size_t index) const {
        bits[getUint64Index(index)] &= ~getBitMask(index);
    }

    template<std::size_t N>
    bool BitMask<N>::testBit(const std::size_t index) const {
        return (bits[getUint64Index(index)] & getBitMask(index)) != 0;
    }

    template<std::size_t N>
    void BitMask<N>::toggleBit(const std::size_t index) const {
        bits[getUint64Index(index)] ^= getBitMask(index);
    }

    template<std::size_t N>
    void BitMask<N>::setBitsTillIdx(const std::size_t index) const {
        const std::size_t completeBlocks = getUint64Index(index);
        const std::size_t remainingBits = getBitPosition(index);

        // Set complete blocks
        for (std::size_t i = 0; i < completeBlocks; ++i) {
            bits[i] = ALL_ONES;
        }

        // Set remaining bits in the partial block (including the idx bit)
        // Always create mask to ensure bit idx is set, even when idx=0
        const uint64_t mask = (1ULL << (remainingBits + 1)) - 1;
        bits[completeBlocks] |= mask;
    }

    template<std::size_t N>
    void BitMask<N>::clearBitsTillIdx(const std::size_t index) const {
        const std::size_t completeBlocks = getUint64Index(index);
        const std::size_t remainingBits = getBitPosition(index);

        // Clear complete blocks
        for (std::size_t i = 0; i < completeBlocks; ++i) {
            bits[i] = 0;
        }

        // Clear remaining bits in the partial block (including the idx bit)
        // Always create mask to ensure bit idx is cleared, even when idx=0
        const uint64_t mask = (1ULL << (remainingBits + 1)) - 1;
        bits[completeBlocks] &= ~mask;
    }

    template<std::size_t N>
    void BitMask<N>::andWith(const BitMask &other) {
        const std::size_t numBlocks = REQUIRED_UINT64<N>;
        for (std::size_t i = 0; i < numBlocks; ++i) {
            bits[i] &= other.bits[i];
        }
    }

    template<std::size_t N>
    void BitMask<N>::copyFrom(const BitMask &other) {
        const std::size_t numBlocks = REQUIRED_UINT64<N>;
        std::memcpy(bits, other.bits, numBlocks * sizeof(uint64_t));
        start_pos = other.start_pos;
        end_pos = other.end_pos;
    }

    template<std::size_t N>
    void BitMask<N>::updatePositions() {
        start_pos = N;
        end_pos = -1;

        std::size_t numBlocks = REQUIRED_UINT64<N>;
        for (std::size_t block = 0; block < numBlocks; ++block) {
            if (bits[block]) {
                const uint64_t val = bits[block];
                for (std::size_t bit = 0; bit < BITS_PER_UINT64 && (block * BITS_PER_UINT64 + bit) < N; ++bit) {
                    if (val & (1ULL << bit)) {
                        auto index = static_cast<int32_t>(block * BITS_PER_UINT64 + bit);
                        start_pos = std::min(start_pos, index);
                        end_pos = std::max(end_pos, index);
                    }
                }
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
#endif

    template class BitMask<2>;
    template class BitMask<4>;
    template class BitMask<8>;
    template class BitMask<16>;
    template class BitMask<32>;
    template class BitMask<64>;
    template class BitMask<128>;
    template class BitMask<256>;
    template class BitMask<512>;
    template class BitMask<1024>;
    template class BitMask<2048>;

} // namespace VFEngine
