//
// Created by Sunny on 21-08-2024.
//
#include "include/datasource.hh"

namespace SampleDB {
    DataSourceTable::DataSourceTable(const std::string &name, const std::vector<std::string> &columns) : _name(name) {
        uint32_t column_idx_id{0};
        _rows = 0;
        _columns = 0;

        for (const auto &column: columns) {
            _column_name_to_index_map[column] = column_idx_id++;
        }
        populate();
        _rows = (int32_t) _table.size();
        _columns = (int32_t) _table[0].size();

    }

    //TODO - make use of the CSVIngestor to fill in the data
    void DataSourceTable::populate() {
        populate_store_with_temporary_data();
    }

    int32_t DataSourceTable::rows_size() const {
        return _rows;
    }

    int32_t DataSourceTable::columns_size() const {
        return _columns;
    }

    void DataSourceTable::populate_store_with_temporary_data() {
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
        for (auto &row: _table) {
            _index[row[0]].push_back(row[1]);
        }
    }
}
