#include "include/datastore.hh"

#include <limits>
#include <memory>

namespace SampleDB {
    static std::unordered_map<int32_t, std::vector<int32_t>> empty_adj_list {};

    DataStore::DataStore(const std::vector<std::string> &table_names,
                         const std::unordered_map<std::string, std::vector<std::string>> &table_to_column_map,
                         const std::unordered_map<std::string, std::string> &column_alias_map) :
        _table_names(table_names), _table_to_column_map(table_to_column_map), _column_alias_map(column_alias_map) {
        populate_tables();
    }

    void DataStore::populate_tables() {
        for (const auto &[table_name, columns]: _table_to_column_map) {
            auto table = std::make_shared<DataSourceTable>(table_name, columns);
            _table_map[table_name] = table;
        }
    }

    std::vector<int32_t> DataStore::get_data_vector_for_column_index(const uint32_t &idx,
                                                                     const std::string &table_name) const {
        std::vector<int32_t> data{};

        auto table_itr = _table_map.find(table_name);

        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            for (auto &data_pair: table->_table) {
                data.push_back(data_pair[idx]);
            }
        }

        return data;
    }

    std::vector<int32_t> DataStore::get_data_vector_for_column(const std::string &column,
                                                               const std::string &table_name) const {
        const auto idx = get_column_idx(table_name, column);
        return get_data_vector_for_column_index(idx, column);
    }

    uint32_t DataStore::get_column_idx(const std::string &column, const std::string &table_name) const {
        auto table_itr = _table_map.find(table_name);

        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            return table->_column_name_to_index_map[column];
        }

        return std::numeric_limits<uint32_t>::max();
    }

    int32_t DataStore::get_table_columns_size(const std::string &table_name) const {
        int32_t size = 0;
        auto table_itr = _table_map.find(table_name);
        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            size = table->columns_size();
        }

        return size;
    }

    int32_t DataStore::get_table_rows_size(const std::string &table_name) const {
        int32_t size = 0;
        auto table_itr = _table_map.find(table_name);
        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            size = table->rows_size();
        }

        return size;
    }

    const std::unordered_map<int32_t, std::vector<int32_t>> &
    DataStore::get_fwd_adj_list(const std::string &table_name) const {
        auto table_itr = _table_map.find(table_name);
        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            return table->get_fwd_adj_list();
        }

        return empty_adj_list;
    }

    const std::unordered_map<int32_t, std::vector<int32_t>> &
    DataStore::get_bwd_adj_list(const std::string &table_name) const {
        auto table_itr = _table_map.find(table_name);
        if (table_itr != _table_map.end()) {
            const auto &table = table_itr->second;
            return table->get_bwd_adj_list();
        }

        return empty_adj_list;
    }

} // namespace SampleDB
