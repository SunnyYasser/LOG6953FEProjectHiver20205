#ifndef SAMPLE_DB_CONTEXT_MEMORY_HH
#define SAMPLE_DB_CONTEXT_MEMORY_HH

#include <cstddef>
#include <unordered_map>
#include <string>
#include "vector.hh"

namespace SampleDB {
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
