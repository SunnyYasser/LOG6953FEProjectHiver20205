//
// Created by Sunny on 21-08-2024.
//
#include "include/datasource.hh"

#include <algorithm>
#include <debug_enabled.hh>
#include <iostream>
#include <sstream>

#include "../data/include/CSVIngestor.hh"
#include "../data/include/serialize_deserialize.hh"
#include "../utils/include/modes.hh"
#include "../utils/include/testpaths.hh"
#include "include/sampledata.hh"


namespace VFEngine {
    size_t size = 10;
    DataSourceTable::DataSourceTable(const std::string &name, const std::vector<std::string> &columns) : _name(name) {
        uint32_t column_idx_id{0};
        for (const auto &column: columns) {
            _column_name_to_index_map[column] = column_idx_id++;
        }

        populate_max_id_value();
        populate_datasource();
    }

    void DataSourceTable::populate_max_id_value() {
        auto filename = get_amazon0601_csv_path();

        if (!filename) {
            std::cerr << "Error opening file for reading number of nodes : " << filename << std::endl;
            _max_id_value = 4; // Return fixed value
            return;
        }

        std::ifstream infile(filename);
        if (!infile.is_open()) {
            std::cerr << "Error opening file for reading number of nodes : " << filename << std::endl;
            _max_id_value = 4; // Return fixed value
            return;
        }

        std::string line;
        const std::string pattern{"Nodes: "};

        while (std::getline(infile, line)) {
            // Look for the line that contains the metadata
            if (line.find("# Nodes:") != std::string::npos && line.find("Edges:") != std::string::npos) {
                std::size_t posNodes = line.find(pattern) + pattern.size(); // Position after "Nodes: "
                _max_id_value = std::stoi(line.substr(posNodes, line.find(' ', posNodes)));
                break;
            }
        }
    }

    void DataSourceTable::populate_datasource() {
        if (is_hot_run_mode_enabled()) {
            read_table_from_data_on_disk();
        } else {
            populate_csv_store();
            if (is_cold_run_mode_enabled()) {
                write_table_as_data_on_disk();
            }
        }
    }

    void DataSourceTable::populate_csv_store() {
        std::vector<std::vector<uint64_t>> _tmp_fwd_adj_list(get_rows_size());
        std::vector<std::vector<uint64_t>> _tmp_bwd_adj_list(get_rows_size());
        const auto file_path = get_amazon0601_csv_path();
        if (!file_path or !CSVIngestionEngine::can_open_file(file_path)) {
            populate_store_with_temporary_data();
        } else {
            std::string line;
            std::ifstream file;
            CSVIngestionEngine::process_file(file_path, file);
            uint64_t src, dest;

            while (std::getline(file, line)) {
                if (line[0] == '#')
                    continue; // Skip comment lines

                std::istringstream iss(line);
                iss >> src >> dest;
                _tmp_fwd_adj_list[src].push_back(dest);
                _tmp_bwd_adj_list[dest].push_back(src);
            }

            file.close();

            for (uint64_t idx = 0; idx <= _max_id_value; ++idx) {
                if (_tmp_fwd_adj_list[idx].empty()) {
                    _tmp_fwd_adj_list[idx] = {};
                }
                if (_tmp_bwd_adj_list[idx].empty()) {
                    _tmp_bwd_adj_list[idx] = {};
                }
            }

            if (is_debug_enabled()) {
                print_adj_list(_tmp_fwd_adj_list);
                print_adj_list(_tmp_bwd_adj_list, true);
            }

            populate_fwd_adj_list(_tmp_fwd_adj_list);
            populate_bwd_adj_list(_tmp_bwd_adj_list);
        }
    }

    void DataSourceTable::print_adj_list(std::vector<std::vector<uint64_t>> &adj_list, bool reverse) const {
        const std::string folder = get_amazon0601_serialized_data_writing_path();
        std::ofstream file;
        if (reverse) {
            const auto &filename = folder + "/bwd_adj_list.txt";
            file.open(filename);
        } else {
            const auto &filename = folder + "/fwd_adj_list.txt";
            file.open(filename);
        }
        if (!file.is_open()) {
            std::cerr << "Could not open for dumping read csv" << std::endl;
            return;
        }

        uint64_t edges = 0;
        file << "Number nodes : " << adj_list.size() << std::endl;
        for (uint64_t idx = 0; idx <= _max_id_value; ++idx) {
            if (adj_list[idx].empty()) {
                continue;
            }
            file << idx << ",";
            if (adj_list[idx].size() > 1) {
                file << "\"";
            }
            edges += adj_list[idx].size();
            std::sort(adj_list[idx].begin(), adj_list[idx].end());

            for (size_t j = 0; j < adj_list[idx].size(); j++) {
                file << adj_list[idx][j];
                if (j != adj_list[idx].size() - 1) {
                    file << ",";
                }
            }

            if (adj_list[idx].size() > 1) {
                file << "\"";
            }

            file << std::endl;
        }
        file << "Number edges : " << edges << std::endl;
        file.close();
    }

    uint64_t DataSourceTable::get_rows_size() const { return _max_id_value + 1; }

    void DataSourceTable::populate_store_with_temporary_data() {
        _fwd_adj_list = std::make_unique<AdjList[]>(5);
        _bwd_adj_list = std::make_unique<AdjList[]>(5);
        populate_sample_data(_fwd_adj_list, _bwd_adj_list);
    }

    void DataSourceTable::populate_fwd_adj_list(const std::vector<std::vector<uint64_t>> &_tmp_adj_list) {
        _fwd_adj_list = std::make_unique<AdjList[]>(_tmp_adj_list.size());
        for (uint64_t src = 0; src < _tmp_adj_list.size(); ++src) {
            const auto &nbrs = _tmp_adj_list[src];
            auto size = nbrs.size();
            AdjList list(size);
            for (auto i = 0; i < size; i++) {
                list._values[i] = nbrs[i];
            }
            _fwd_adj_list[src] = std::move(list);
        }
    }

    void DataSourceTable::populate_bwd_adj_list(const std::vector<std::vector<uint64_t>> &_tmp_adj_list) {
        _bwd_adj_list = std::make_unique<AdjList[]>(_tmp_adj_list.size());
        for (uint64_t src = 0; src < _tmp_adj_list.size(); ++src) {
            const auto &nbrs = _tmp_adj_list[src];
            auto size = nbrs.size();
            AdjList list(size);
            for (auto i = 0; i < size; i++) {
                list._values[i] = nbrs[i];
            }
            _bwd_adj_list[src] = std::move(list);
        }
    }

    void DataSourceTable::read_table_from_data_on_disk() {
        if (is_deserializing_mode_disabled()) {
            return;
        }
        const auto &filepath = get_amazon0601_serialized_data_reading_path();
        if (!filepath) {
            return;
        }

        _fwd_adj_list = std::make_unique<AdjList[]>(get_rows_size());
        _bwd_adj_list = std::make_unique<AdjList[]>(get_rows_size());
        const SerializeDeserialize<uint64_t> engine{filepath, this};
        engine.deserialize();

        print_adj_list(_fwd_adj_list);
        print_adj_list(_bwd_adj_list, true);
    }

    void DataSourceTable::print_adj_list(const std::unique_ptr<AdjList[]> &adj_list, bool reverse) const {
        const std::string folder = get_amazon0601_serialized_data_reading_path();
        std::ofstream file;
        if (reverse) {
            const auto &filename = folder + "/deserialized_bwd_adj_list.txt";
            file.open(filename);
        } else {
            const auto &filename = folder + "/deserialized_fwd_adj_list.txt";
            file.open(filename);
        }
        if (!file.is_open()) {
            std::cerr << "Could not open for dumping read csv" << std::endl;
            return;
        }

        uint64_t edges = 0;
        file << "Number nodes : " << get_rows_size() << std::endl;

        for (uint64_t idx = 0; idx <= _max_id_value; ++idx) {
            if (adj_list[idx]._size == 0) {
                continue;
            }
            file << idx << ",";
            if (adj_list[idx]._size > 1) {
                file << "\"";
            }

            uint64_t adj_list_size = adj_list[idx]._size;
            edges += adj_list_size;

            uint64_t values_copy[adj_list_size]{};
            std::copy(adj_list[idx]._values, adj_list[idx]._values + adj_list_size, values_copy);
            std::sort(values_copy, values_copy + adj_list_size);

            for (size_t j = 0; j < adj_list_size; j++) {
                file << values_copy[j];
                if (j != adj_list_size - 1) {
                    file << ",";
                }
            }

            if (adj_list_size > 1) {
                file << "\"";
            }

            file << std::endl;
        }
        file << "Number edges : " << edges << std::endl;
        file.close();
    }


    void DataSourceTable::write_table_as_data_on_disk() const {
        if (is_serializing_mode_disabled()) {
            return;
        }
        const auto &filepath = get_amazon0601_serialized_data_writing_path();
        if (!filepath) {
            return;
        }
        const SerializeDeserialize<uint64_t> engine{filepath, this};
        engine.serialize();
    }

    const std::unique_ptr<AdjList[]> &DataSourceTable::get_fwd_adj_list() const { return _fwd_adj_list; }

    const std::unique_ptr<AdjList[]> &DataSourceTable::get_bwd_adj_list() const { return _bwd_adj_list; }

    uint64_t DataSourceTable::get_max_id_value() const { return _max_id_value; }

} // namespace VFEngine
