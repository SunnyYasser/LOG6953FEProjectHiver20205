#include <utility>
#include "include/context.hh"

namespace SampleDB {
    void ContextMemory::allocate_memory_for_column(const std::string &table_name, const std::string &column) {
        const auto &table_itr = _context.find(table_name);

        if (table_itr == _context.end()) {
            _context[table_name] = {};
        }

        auto &table = _context[table_name];

        table.try_emplace(column, State::MAX_VECTOR_SIZE);
    }

    bool ContextMemory::update_column_data(const std::string &table_name, const std::string &column, Vector vector) {
        if (_context.find(table_name) == _context.end()) {
            return false;
        }

        auto &table = _context[table_name];
        if (table.find(column) == table.end()) {
            return false;
        }

        table[column] = std::move(vector);
        return true;
    }

    Vector ContextMemory::read_vector_for_column(const std::string &table_name, const std::string &column, bool& result) {
        const auto &table_itr = _context.find(table_name);

        if (table_itr == _context.end()) {
            return {};
        }

        auto &table = _context[table_name];
        if (table.find(column) == table.end()) {
            return {};
        }

        auto vec = table[column];
        result = true;
        return vec;
    }

    Vector ContextMemory::read_vector_for_column(const std::string &table_name, const std::string &column) {
        bool result {false};
        return read_vector_for_column(table_name, column, result);
    }

}
