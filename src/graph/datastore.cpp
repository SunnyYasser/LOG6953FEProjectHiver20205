#include <limits.h>
#include "include/datastore.hh"
namespace SampleDB
{
    DataStore::DataStore(const std::vector<std::string> &columns) : _columns(columns)
    {
        uint32_t column_idx_id{0};
        for (const auto &column : _columns)
        {
            _column_name_to_index_map[column] = column_idx_id++;
        }
    }

    //TODO - delete this API
    void DataStore::populate_store_with_temporary_data()
    {
        /* for execute testing purposes*/
        _datastore.push_back({1, 2});
        _datastore.push_back({1, 3});
        _datastore.push_back({1, 4});
        _datastore.push_back({2, 3});
        _datastore.push_back({2, 4});
        _datastore.push_back({3, 4});

        populate_index();
    }

    void DataStore::populate_index()
    {
        for (auto &row : _datastore)
        {
            _index[row[0]].push_back(row[1]);
        }
    }

    const std::vector<int32_t> DataStore::get_data_vector_for_column_index(uint32_t idx) const
    {
        std::vector<int32_t> data;
        for (auto &data_pair : _datastore)
        {
            data.push_back(data_pair[idx]);
        }

        return data;
    }

    const std::vector<int32_t> DataStore::get_data_vector_for_column(const std::string &column) const
    {
        auto idx = _column_name_to_index_map[column];
        return get_data_vector_for_column_index(idx);
    }

    const std::vector<int32_t> DataStore::get_values_from_index(int32_t value) const
    {
        return _datastore[value];
    }
}