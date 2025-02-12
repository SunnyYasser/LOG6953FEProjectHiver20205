#ifndef VFENGINE_ARENA_ALLOCATOR_HH
#define VFENGINE_ARENA_ALLOCATOR_HH

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>

namespace VFEngine {
    class ArenaAllocator {
    public:
        static ArenaAllocator &getInstance();
        void initialize(size_t total_size);
        void *allocate(size_t size);

        ArenaAllocator(const ArenaAllocator &) = delete;
        ArenaAllocator &operator=(const ArenaAllocator &) = delete;
        ArenaAllocator(ArenaAllocator &&) = delete;
        ArenaAllocator &operator=(ArenaAllocator &&) = delete;

    private:
        ArenaAllocator();
        ~ArenaAllocator() = default;
        std::unique_ptr<uint8_t[]> _memory_pool;
        size_t _total_size;
        std::atomic<size_t> _current_offset;
    };

    class ArenaSetup {
    public:
        static void initialize(size_t total_size);
    };
} // namespace VFEngine

#endif