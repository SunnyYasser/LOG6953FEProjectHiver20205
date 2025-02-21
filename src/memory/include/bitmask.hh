#ifndef BITMASK_HH
#define BITMASK_HH

#include <array>
#include <cstdint>

namespace VFEngine {

#ifdef BIT_ARRAY_AS_FILTER
    // BitArrayMask implementation (one byte per bit)
    template<std::size_t N>
    class BitMask {
    public:
        static_assert((N & (N - 1)) == 0, "Size must be a power of 2");

        BitMask();
        BitMask(const BitMask &other);
        BitMask &operator=(const BitMask &other);
        void setBit(std::size_t index);
        void clearBit(std::size_t index);
        bool testBit(std::size_t index) const;
        void toggleBit(std::size_t index);
        void clearAllBits();
        void setAllBits();
        void andWith(const BitMask &other);
        void copyFrom(const BitMask &other);
        static constexpr std::size_t size() { return N; }

    private:
        std::array<uint8_t, N> bits{};
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
        void setBit(std::size_t index);
        void clearBit(std::size_t index);
        bool testBit(std::size_t index) const;
        void toggleBit(std::size_t index);
        void clearAllBits();
        void setAllBits();
        void andWith(const BitMask &other);
        void copyFrom(const BitMask &other);
        static constexpr std::size_t getBitPosition(const std::size_t index) { return index & BIT_MASK_63; }

        static constexpr std::size_t getUint64Index(const std::size_t index) { return index >> 6; }

        static constexpr uint64_t getBitMask(const std::size_t index) { return 1ULL << getBitPosition(index); }

        static constexpr std::size_t size() { return N; }

    private:
        std::array<uint64_t, REQUIRED_UINT64<N>> bits{};
    };
#endif

} // namespace VFEngine

#define SET_BIT(bitmask, index) ((bitmask).setBit(index))
#define CLEAR_BIT(bitmask, index) ((bitmask).clearBit(index))
#define TEST_BIT(bitmask, index) ((bitmask).testBit(index))
#define TOGGLE_BIT(bitmask, index) ((bitmask).toggleBit(index))
#define CLEAR_ALL_BITS(bitmask) ((bitmask).clearAllBits())
#define SET_ALL_BITS(bitmask) ((bitmask).setAllBits())
#define AND_BITMASKS(N, first, second) ((first).andWith(second))
#define RESET_BITMASK(N, first, second) ((first).copyFrom(second))
#endif
