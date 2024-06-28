#include <assert.h>
#include <cstring>
#include "include/context.hh"

namespace SampleDB
{
    void ContextMemory::allocate_memory_for_operator(const std::string &column)
    {
        if (_context.find (column) != _context.end())
        {
            // already exists
            return;
        }

        Vector vector {State::MAX_VECTOR_SIZE};
        _context[column] = vector;
    }

    bool ContextMemory::update_operator_data(const std::string &column, Vector vector)
    {
        if (_context.find(column) == _context.end())
        {
            return false;
        }

        _context[column] = vector;
        return true;
    }

    Vector ContextMemory::read_vector_for_attribute (const std::string &column)
    {
        auto vec = _context[column];
        return vec;
    }

}