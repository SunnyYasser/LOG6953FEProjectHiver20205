//
// Created by Sunny on 21-08-2024.
//
#include "include/datasource.hh"
#include "../data/include/CSVIngestor.hh"
#include "../data/include/serialize_deserialize.hh"
#include "../utils/include/testpaths.hh"

namespace VFEngine {
    DataSourceTable::DataSourceTable(const std::string &name, const std::vector<std::string> &columns) :
        _name(name), _fwd_adj_list({}), _bwd_adj_list({}) {

        uint32_t column_idx_id{0};
        _rows = 0;
        _columns = 0;

        for (const auto &column: columns) {
            _column_name_to_index_map[column] = column_idx_id++;
        }

        populate_store();

        _rows = static_cast<int32_t>(_table.size());
        _columns = static_cast<int32_t>(_table[0].size());

        populate_adj_list();
    }

    void DataSourceTable::populate_store() {
        const std::string file = get_fb_0edges_path();
        if (VFEngine::CSVIngestionEngine::can_open_file(file)) {
            std::ifstream file_handle;
            VFEngine::CSVIngestionEngine::process_file(file, file_handle);
            int src, dest;

            while (file_handle >> src >> dest) {
                _table.push_back({src, dest});
            }

            file_handle.close();

        } else {
            populate_store_with_temporary_data();
        }
    }

    int32_t DataSourceTable::rows_size() const { return _rows; }

    int32_t DataSourceTable::columns_size() const { return _columns; }

    void DataSourceTable::populate_store_with_temporary_data() {
        /* for execute testing purposes*/
        _table.push_back({1, 2});
        _table.push_back({1, 3});
        _table.push_back({1, 4});
        _table.push_back({2, 3});
        _table.push_back({2, 4});
        _table.push_back({3, 4});
    }

    void DataSourceTable::populate_adj_list() {
        populate_fwd_adj_list();
        populate_bwd_adj_list();
    }

    void DataSourceTable::populate_fwd_adj_list() {
        for (auto &row: _table) {
            _fwd_adj_list[row[0]].push_back(row[1]);
        }
    }

    void DataSourceTable::populate_bwd_adj_list() {
        for (auto &row: _table) {
            _bwd_adj_list[row[1]].push_back(row[0]);
        }
    }

    void DataSourceTable::read_table_from_data_on_disk(const std::string &filepath) {
        SerializeDeserialize<int> engine{filepath};
        _table = engine.deserializeVector();
    }

    void DataSourceTable::write_table_as_data_on_disk(const std::string &filepath) const {
        SerializeDeserialize<int> engine{filepath};
        engine.serializeVector(_table);
    }

    const std::unordered_map<int32_t, std::vector<int32_t>> &DataSourceTable::get_fwd_adj_list() const {
        return _fwd_adj_list;
    }

    const std::unordered_map<int32_t, std::vector<int32_t>> &DataSourceTable::get_bwd_adj_list() const {
        return _bwd_adj_list;
    }


} // namespace VFEngine
