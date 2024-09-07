#ifndef VFENGINE_CONTEXT_MEMORY_HH
#define VFENGINE_CONTEXT_MEMORY_HH

#include <unordered_map>
#include <string>
#include "vector.hh"

namespace VFEngine {
    class ContextMemory {
    public:
        ContextMemory() = default;

        ContextMemory(ContextMemory &&) = delete;

        ContextMemory(const ContextMemory &) = delete;

    public:
        void allocate_memory_for_column(const std::string & column, const std::string & table = "R");

        bool update_column_data(const std::string & column, const Vector&, const std::string & table = "R");

        Vector& read_vector_for_column(const std::string & column, const std::string & table= "R");

    private:
        // <table_name : <column : vector>>
        std::unordered_map<std::string, std::unordered_map<std::string, Vector> > _context;
    };
}

#endif
