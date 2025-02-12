//
// Created by sunny on 9/12/24.
//
#include "include/adjlist.hh"

#include <cstring>

namespace VFEngine {
    AdjList::AdjList() : _size(0), _values_uptr(nullptr), _values(nullptr) {}

    AdjList::AdjList(const std::size_t &size) : _size(size), _values_uptr(std::make_unique<uint64_t[]>(size)) {
        _values = _values_uptr.get();
        memset(_values, 0, size * sizeof(uint64_t));
    }
} // namespace VFEngine
