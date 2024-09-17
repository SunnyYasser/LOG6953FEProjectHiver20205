//
// Created by sunny on 9/12/24.
//
#include "include/adjlist.hh"

namespace VFEngine {
    AdjList::AdjList() : _size(0), _values_uptr(nullptr), _values(nullptr) {}

    AdjList::AdjList(const std::size_t &size) : _size(size), _values_uptr(std::make_unique<uint64_t[]>(size)) {
        _values = _values_uptr.get();
        for (size_t i = 0; i < size; i++) {
            _values[i] = 0;
        }
    }
} // namespace VFEngine
