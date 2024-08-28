#include "include/datastore.hh"

#include <limits>
#include <memory>

namespace SampleDB {
    DataStore::DataStore(const std::vector<std::string> &table_names,
                         const std::unordered_map<std::string, std::vector<std::string> > &
                         table_to_column_map) : _table_names(table_names), _table_to_column_map(table_to_column_map) {
        populate_tables();
    }

    void DataStore::populate_tables() {
        for (const auto &[table_name, columns]: _table_to_column_map) {
            auto table = std::make_shared<DataSourceTable>(table_name, columns);
            _table_map[table_name] = table;
        }
    }

    std::vector<int32_t>
    DataStore::get_data_vector_for_column_index(const std::string &table_name, const uint32_t idx) const {
        std::vector<int32_t> data {};

        auto table_itr = _table_map.find(table_name);

        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            for (auto &data_pair: table->_table) {
                data.push_back(data_pair[idx]);
            }
        }

        return data;
    }

    std::vector<int32_t>
    DataStore::get_data_vector_for_column(const std::string &table_name, const std::string &column) const {
        const auto idx = get_column_idx(table_name, column);
        return get_data_vector_for_column_index(table_name, idx);
    }

    std::vector<int32_t> DataStore::get_values_from_table_index(const std::string &table_name,
                                                                const int32_t value) const {
        std::vector<int32_t> data {};
        auto table_itr = _table_map.find(table_name);

        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            data = table->_index[value];
        }

        return data;
    }

    uint32_t DataStore::get_column_idx(const std::string &table_name, const std::string &column) const {
        auto table_itr = _table_map.find(table_name);

        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            return table->_column_name_to_index_map[column];
        }

        return std::numeric_limits<uint32_t>::max();
    }

    int32_t DataStore::get_table_columns_size(const std::string &table) const {
        int32_t size = 0;
        auto table_itr = _table_map.find(table);
        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            size = table->columns_size();
        }

        return size;
    }

    int32_t DataStore::get_table_rows_size(const std::string &table) const {
        int32_t size = 0;
        auto table_itr = _table_map.find(table);
        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            size = table->rows_size();
        }

        return size;
    }

}
