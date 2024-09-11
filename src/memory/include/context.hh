#ifndef VFENGINE_CONTEXT_MEMORY_HH
#define VFENGINE_CONTEXT_MEMORY_HH

#include <string>
#include <unordered_map>
#include "vector.hh"

namespace VFEngine {
    class ContextMemory {
    public:
        ContextMemory() = default;
        ContextMemory(ContextMemory &&) = delete;
        ContextMemory(const ContextMemory &) = delete;
        void allocate_memory_for_column(const std::string &column);
        Vector* read_vector_for_column(const std::string &column);

    private:
        // <table_name : <column : vector>>
        std::unordered_map<std::string, Vector> _context;
    };
} // namespace VFEngine

#endif
