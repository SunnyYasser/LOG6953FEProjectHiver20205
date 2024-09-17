//
// Created by sunny on 9/12/24.
//

#ifndef VFENGINE_ADJLIST_HH
#define VFENGINE_ADJLIST_HH

#include <cstddef>
#include <cstdint>
#include <memory>


namespace VFEngine {
    class AdjList {
    public:
        AdjList();
        explicit AdjList(const std::size_t &size);
        std::size_t _size;
        uint64_t *_values;

    private:
        std::unique_ptr<uint64_t[]> _values_uptr;
    };
} // namespace VFEngine

#endif
