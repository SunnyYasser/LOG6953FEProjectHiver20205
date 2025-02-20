#include <array>
#include <cstdint>
#include <cstring>

constexpr std::size_t BITS_PER_UINT64 = 64;
constexpr uint64_t ALL_ONES = ~0ULL;
constexpr uint64_t BIT_MASK_63 = BITS_PER_UINT64 - 1; // 63 = 0b111111

template<std::size_t N>
constexpr std::size_t REQUIRED_UINT64 = (N + BITS_PER_UINT64 - 1) / BITS_PER_UINT64;

template<std::size_t N>
struct BitMask {
    static_assert((N & (N - 1)) == 0, "Size must be a power of 2");
    std::array<uint64_t, REQUIRED_UINT64<N>> bits{};
};

#define GET_BIT_POSITION(index) ((index) & BIT_MASK_63)
#define GET_UINT64_INDEX(index) ((index) >> 6)
#define BIT_MASK(index) (1ULL << GET_BIT_POSITION(index))

#define SET_BIT(bitmask, index) ((bitmask).bits[GET_UINT64_INDEX(index)] |= BIT_MASK(index))

#define CLEAR_BIT(bitmask, index) ((bitmask).bits[GET_UINT64_INDEX(index)] &= ~BIT_MASK(index))

#define TEST_BIT(bitmask, index) (((bitmask).bits[GET_UINT64_INDEX(index)] & BIT_MASK(index)) != 0)

#define TOGGLE_BIT(bitmask, index) ((bitmask).bits[GET_UINT64_INDEX(index)] ^= BIT_MASK(index))

#define CLEAR_ALL_BITS(bitmask) std::memset((bitmask).bits.data(), 0, sizeof((bitmask).bits))

#define SET_ALL_BITS(bitmask) std::memset((bitmask).bits.data(), 0xFF, sizeof((bitmask).bits))

#define AND_BITMASKS(N, first, second)                                                                                 \
    do {                                                                                                               \
        uint64_t *__restrict__ first_ptr = (first).bits.data();                                                        \
        const uint64_t *__restrict__ second_ptr = (second).bits.data();                                                \
        for (size_t i = 0; i < REQUIRED_UINT64<N>; i++) {                                                              \
            first_ptr[i] &= second_ptr[i];                                                                             \
        }                                                                                                              \
    } while (0)

#define RESET_BITMASK(N, first, second)                                                                                \
    do {                                                                                                               \
        uint64_t *__restrict__ first_ptr = (first).bits.data();                                                        \
        const uint64_t *__restrict__ second_ptr = (second).bits.data();                                                \
        for (size_t i = 0; i < REQUIRED_UINT64<N>; i++) {                                                              \
            first_ptr[i] = second_ptr[i];                                                                              \
        }                                                                                                              \
    } while (0)
