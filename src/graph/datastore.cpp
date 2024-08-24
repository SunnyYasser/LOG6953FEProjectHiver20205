#include <limits.h>
#include "include/datastore.hh"
namespace SampleDB
{
    DataStore::DataStore(const std::vector<std::string> &table_names) : _table_names (table_names)
    {
    }

    std::vector<int32_t> DataStore::get_data_vector_for_column_index(const std::string& table_name, const uint32_t idx)
    {
        std::vector<int32_t> data;
        auto& table = _table_map [table_name];
        for (auto &data_pair : table._table)
        {
            data.push_back(data_pair[idx]);
        }

        return data;
    }

    std::vector<int32_t> DataStore::get_data_vector_for_column(const std::string& table_name, const std::string &column)
    {
        const auto idx = get_column_idx (table_name,column);
        return get_data_vector_for_column_index(table_name, idx);
    }

    std::vector<int32_t> DataStore::get_values_from_index(const std::string& table_name, const int32_t value)
    {
        auto& table = _table_map [table_name];
        return table._index[value];
    }

    uint32_t DataStore::get_column_idx (const std::string& table_name, const std::string& column)
    {
        auto& table = _table_map [table_name];
        return table._column_name_to_index_map [column];
    }
}