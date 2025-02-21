#include "bitmask.hh"
#include <cstring>
namespace VFEngine {

#ifdef BIT_ARRAY_AS_FILTER

    template<std::size_t N>
    BitMask<N>::BitMask() {
        setAllBits();
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) {
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
    void BitMask<N>::setBit(std::size_t index) {
        bits[index] = 1;
    }

    template<std::size_t N>
    void BitMask<N>::clearBit(std::size_t index) {
        bits[index] = 0;
    }

    template<std::size_t N>
    bool BitMask<N>::testBit(std::size_t index) const {
        return bits[index] != 0;
    }

    template<std::size_t N>
    void BitMask<N>::toggleBit(std::size_t index) {
        bits[index] = !bits[index];
    }

    template<std::size_t N>
    void BitMask<N>::clearAllBits() {
        for (auto i=0; i<N; i++) bits[i] = 0;
        //std::memset(bits.data(), 0, sizeof(bits));
    }

    template<std::size_t N>
    void BitMask<N>::setAllBits() {
        for (auto i=0; i<N; i++) bits[i] = 1;
        //std::memset(bits.data(), 1, sizeof(bits));
    }

    template<std::size_t N>
    void BitMask<N>::andWith(const BitMask &other) {
        for (std::size_t i = 0; i < N; ++i) {
            bits[i] = bits[i] && other.bits[i];
        }
    }

    template<std::size_t N>
    void BitMask<N>::copyFrom(const BitMask &other) {
        for (auto i=0; i<N; i++)
            bits[i] = other.bits[i];
        //std::memcpy(bits.data(), other.bits.data(), sizeof(bits));
    }

#else

    template<std::size_t N>
    BitMask<N>::BitMask() {
        setAllBits();
    }

    template<std::size_t N>
    BitMask<N>::BitMask(const BitMask &other) {
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
    void BitMask<N>::setBit(std::size_t index) {
        bits[getUint64Index(index)] |= getBitMask(index);
    }

    template<std::size_t N>
    void BitMask<N>::clearBit(std::size_t index) {
        bits[getUint64Index(index)] &= ~getBitMask(index);
    }

    template<std::size_t N>
    bool BitMask<N>::testBit(std::size_t index) const {
        return (bits[getUint64Index(index)] & getBitMask(index)) != 0;
    }

    template<std::size_t N>
    void BitMask<N>::toggleBit(std::size_t index) {
        bits[getUint64Index(index)] ^= getBitMask(index);
    }

    template<std::size_t N>
    void BitMask<N>::clearAllBits() {
        std::memset(bits.data(), 0, sizeof(bits));
    }

    template<std::size_t N>
    void BitMask<N>::setAllBits() {
        std::memset(bits.data(), 0xFF, sizeof(bits));
    }

    template<std::size_t N>
    void BitMask<N>::andWith(const BitMask &other) {
        for (std::size_t i = 0; i < REQUIRED_UINT64<N>; ++i) {
            bits[i] &= other.bits[i];
        }
    }

    template<std::size_t N>
    void BitMask<N>::copyFrom(const BitMask &other) {
        std::memcpy(bits.data(), other.bits.data(), sizeof(bits));
    }

#endif

    template class BitMask<1024>;
    template class BitMask<2048>;

} // namespace VFEngine
