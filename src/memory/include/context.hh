#ifndef SAMPLE_DB_CONTEXT_MEMORY_HH
#define SAMPLE_DB_CONTEXT_MEMORY_HH

#include <cstddef>
#include <unordered_map>
#include <string>
#include "vector.hh"

namespace SampleDB
{
    class ContextMemory
    {

    public:
        ContextMemory() = default;
        ContextMemory(ContextMemory &&) = delete;
        ContextMemory(const ContextMemory &) = delete;

    public:
        void allocate_memory_for_operator(const std::string &);
        bool update_operator_data(const std::string &, Vector);
        Vector read_vector_for_attribute (const std::string &);

    private:

        std::unordered_map<std::string, Vector> _context;
    };

}

#endif
