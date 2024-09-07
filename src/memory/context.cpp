#include "include/context.hh"

namespace VFEngine {
    static Vector tmp{};
    void ContextMemory::allocate_memory_for_column(const std::string &column, const std::string &table_name) {
        const auto &table_itr = _context.find(table_name);

        if (table_itr == _context.end()) {
            _context[table_name] = {};
        }

        auto &table = _context[table_name];

        if (table.find(column) != table.end()) {
            return;
        }

        table.try_emplace(column, State::MAX_VECTOR_SIZE);
    }

    bool ContextMemory::update_column_data(const std::string &column, const Vector &new_vector,
                                           const std::string &table_name) {
        if (_context.find(table_name) == _context.end()) {
            return false;
        }

        auto &table = _context[table_name];
        if (table.find(column) == table.end()) {
            return false;
        }

        auto &original_vector = table[column];
        original_vector._state->_pos = new_vector._state->_pos;
        original_vector._state->_size = new_vector._state->_size;
        original_vector._vector = new_vector._vector;


        return true;
    }

    Vector &ContextMemory::read_vector_for_column(const std::string &column, const std::string &table_name) {
        const auto &table_itr = _context.find(table_name);

        if (table_itr == _context.end()) {
            return tmp;
        }

        auto &table = _context[table_name];
        if (table.find(column) == table.end()) {
            return tmp;
        }

        auto &vec = table[column];
        return vec;
    }

} // namespace VFEngine
