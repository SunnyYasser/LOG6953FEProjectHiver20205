//
// Created by Sunny on 21-08-2024.
//
#include "include/datasource.hh"
namespace SampleDB {

    DataSourceTable::DataSourceTable(std::vector<std::string> & columns) {
        uint32_t column_idx_id{0};
        for (const auto &column : columns)
        {
            _column_name_to_index_map[column] = column_idx_id++;
        }

    }

    void DataSourceTable::populate_store_with_temporary_data () {
        /* for execute testing purposes*/
        _table.push_back({1, 2});
        _table.push_back({1, 3});
        _table.push_back({1, 4});
        _table.push_back({2, 3});
        _table.push_back({2, 4});
        _table.push_back({3, 4});

        populate_index();
    }

    void DataSourceTable::populate_index() {
        for (auto &row : _table)
        {
            _index[row[0]].push_back(row[1]);
        }
    }
}