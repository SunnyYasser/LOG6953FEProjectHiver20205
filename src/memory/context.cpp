#include "include/context.hh"

namespace VFEngine {
    static Vector tmp{};

    void ContextMemory::allocate_memory_for_column(const std::string &column) {
        if (_context.find(column) != _context.end()) {
            return;
        }

        _context.try_emplace(column);
    }

    Vector* ContextMemory::read_vector_for_column(const std::string &column) {
        if (_context.find(column) == _context.end()) [[unlikely]] {
            return &tmp;
        }

        auto &vec = _context[column];
        return &vec;
    }

} // namespace VFEngine
