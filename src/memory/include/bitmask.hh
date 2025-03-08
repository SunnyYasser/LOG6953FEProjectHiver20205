#ifndef BITMASK_HH
#define BITMASK_HH

#include <array>
#include <cstdint>
#include <memory>
namespace VFEngine {

#ifdef BIT_ARRAY_AS_FILTER
    // BitArrayMask implementation (one byte per bit)
    template<std::size_t N>
    class BitMask {
    public:
        static_assert((N & (N - 1)) == 0, "Size must be a power of 2");

        BitMask();
        ~BitMask() = default;
        BitMask(const BitMask &other);
        BitMask &operator=(const BitMask &other);
        void setBit(std::size_t index) const;
        void clearBit(std::size_t index) const;
        [[nodiscard]] bool testBit(std::size_t index) const;
        void toggleBit(std::size_t index) const;
        void clearBitsTillIdx(size_t index) const;
        void setBitsTillIdx(size_t index) const;
        void andWith(const BitMask &other);
        void copyFrom(const BitMask &other);
        static constexpr std::size_t size() { return N; }
        [[nodiscard]] int32_t getStartPos() const;
        [[nodiscard]] int32_t getEndPos() const;
        void updatePositions();
        void setStartPos(int32_t idx_value);
        void setEndPos(int32_t idx_value);

    private:
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        uint8_t *bits;
#else
        std::unique_ptr<uint8_t[]> bits_uptr;
        uint8_t *bits;
#endif
        int32_t start_pos, end_pos;
    };

#else
    // BitMask implementation (bit packing in uint64_t)
    constexpr std::size_t BITS_PER_UINT64 = 64;
    constexpr uint64_t ALL_ONES = ~0ULL;
    constexpr uint64_t BIT_MASK_63 = BITS_PER_UINT64 - 1; // 63 = 0b111111

    template<std::size_t N>
    constexpr std::size_t REQUIRED_UINT64 = (N + BITS_PER_UINT64 - 1) / BITS_PER_UINT64;

    template<std::size_t N>
    class BitMask {
    public:
        static_assert((N & (N - 1)) == 0, "Size must be a power of 2");
        BitMask();
        BitMask(const BitMask &other);
        BitMask &operator=(const BitMask &other);
        void setBit(std::size_t index) const;
        void clearBit(std::size_t index) const;
        [[nodiscard]] bool testBit(std::size_t index) const;
        void toggleBit(std::size_t index) const;
        void clearBitsTillIdx(size_t index) const;
        void setBitsTillIdx(size_t index) const;
        void andWith(const BitMask &other);
        void copyFrom(const BitMask &other);
        void updatePositions();
        static constexpr std::size_t getBitPosition(const std::size_t index) { return index & BIT_MASK_63; }
        static constexpr std::size_t getUint64Index(const std::size_t index) { return index >> 6; }
        static constexpr uint64_t getBitMask(const std::size_t index) { return 1ULL << getBitPosition(index); }
        static constexpr std::size_t size() { return N; }
        [[nodiscard]] int32_t getStartPos() const;
        [[nodiscard]] int32_t getEndPos() const;
        void setStartPos(int32_t idx_value);
        void setEndPos(int32_t idx_value);

    private:
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
        uint64_t *bits;
#else
        std::unique_ptr<uint64_t[]> bits_uptr;
        uint64_t *bits;
#endif
        int32_t start_pos, end_pos;
    };
#endif

} // namespace VFEngine

#define SET_BIT(bitmask, index) ((bitmask).setBit(index))
#define CLEAR_BIT(bitmask, index) ((bitmask).clearBit(index))
#define TEST_BIT(bitmask, index) ((bitmask).testBit(index))
#define TOGGLE_BIT(bitmask, index) ((bitmask).toggleBit(index))
#define CLEAR_ALL_BITS(bitmask) ((bitmask).clearBitsTillIdx((bitmask).size() - 1))
#define SET_ALL_BITS(bitmask) ((bitmask).setBitsTillIdx((bitmask).size() - 1))
#define SET_BITS_TILL_IDX(bitmask, idx) ((bitmask).setBitsTillIdx(idx))
#define CLEAR_BITS_TILL_IDX(bitmask, idx) ((bitmask).clearBitsTillIdx(idx))
#define AND_BITMASKS(N, first, second) ((first).andWith(second))
#define COPY_BITMASK(N, first, second) ((first).copyFrom(second))
#define GET_START_POS(bitmask) ((bitmask).getStartPos())
#define GET_END_POS(bitmask) ((bitmask).getEndPos())
#define SET_START_POS(bitmask, index) ((bitmask).setStartPos(index))
#define SET_END_POS(bitmask, index) ((bitmask).setEndPos(index))
#define UPDATE_POSITIONS(bitmask) ((bitmask).updatePositions())
#endif
