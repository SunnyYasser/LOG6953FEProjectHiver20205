#include "include/context.hh"

namespace VFEngine {
    void ContextMemory::allocate_memory_for_column(const std::string &column) {
        if (_context.find(column) != _context.end()) {
            return;
        }

        _context.try_emplace(column);
    }

    void ContextMemory::allocate_memory_for_column(const std::string &column, const std::string &parent_column,
                                                   const bool &share_state) {
        if (_context.find(column) != _context.end()) {
            return;
        }

        if (share_state) {
            auto pvec = read_vector_for_column(parent_column);
            _context.try_emplace(column, pvec->_state);
            return;
        }

        _context.try_emplace(column);
    }


    Vector *ContextMemory::read_vector_for_column(const std::string &column) {
        auto &vec = _context[column];
        return &vec;
    }

} // namespace VFEngine
