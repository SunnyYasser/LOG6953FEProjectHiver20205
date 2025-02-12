//
// Created by Sunny on 21-08-2024.
//
#include "include/datasource.hh"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <sstream>
#include "../data/include/CSVIngestor.hh"
#include "../data/include/serialize_deserialize.hh"
#include "../debug/include/memory_debug.hh"
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

        if (!is_cold_run_mode_enabled() && !is_hot_run_mode_enabled()) {
            _max_id_value = DEFAULT_MAX_ID_VALUE;
            return;
        }

        auto filename = get_dataset_csv_path();

        if (!filename) {
            std::cerr << "Error opening file for reading number of nodes : " << filename << std::endl;
            _max_id_value = DEFAULT_MAX_ID_VALUE;
            return;
        }

        std::ifstream infile(filename);
        if (!infile.is_open()) {
            std::cerr << "Error opening file for reading number of nodes : " << filename << std::endl;
            _max_id_value = DEFAULT_MAX_ID_VALUE;
            return;
        }

        std::string line;
        uint64_t max_id = 0;
        uint64_t src, dest;

        while (std::getline(infile, line)) {
            if (line[0] == '#')
                continue;

            std::istringstream iss(line);
            if (iss >> src >> dest) {
                max_id = std::max({max_id, src, dest});
            }
        }
        infile.close();
        _max_id_value = max_id;
    }

    inline void benchmark_barrier() {
        std::atomic_thread_fence(std::memory_order_seq_cst); // prevent hardware reordering
        asm volatile("" ::: "memory"); // prevent compiler reordering (only for gcc)
    }

    void DataSourceTable::populate_datasource() {
        benchmark_barrier();
        auto exec_start_time = std::chrono::steady_clock::now();

        if (is_hot_run_mode_enabled()) {
            read_table_from_data_on_disk();
        } else {
            populate_csv_store();
            if (is_cold_run_mode_enabled()) {
                write_table_as_data_on_disk();
            }
        }

        benchmark_barrier();
        auto exec_end_time = std::chrono::steady_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(exec_end_time - exec_start_time);

#ifdef MY_DEBUG
        MemoryDebugUtility::print_adj_list(_fwd_adj_list, get_max_id_value());
        MemoryDebugUtility::print_adj_list(_bwd_adj_list, get_max_id_value(), true);
        MemoryDebugUtility::print_serialize_deseerialize_time(duration.count());
#endif
    }

    void DataSourceTable::populate_csv_store() {
        const auto file_path = get_dataset_csv_path();
        if (!file_path or !CSVIngestionEngine::can_open_file(file_path)) {
            populate_store_with_temporary_data();
            return;
        }

        // First pass: Count edges per vertex to pre-allocate
        auto fwd_counts = std::make_unique<size_t[]>(get_rows_size());
        auto bwd_counts = std::make_unique<size_t[]>(get_rows_size());

        std::memset(fwd_counts.get(), 0, get_rows_size() * sizeof(size_t));
        std::memset(bwd_counts.get(), 0, get_rows_size() * sizeof(size_t));

        {
            std::ifstream file;
            CSVIngestionEngine::process_file(file_path, file);
            std::string line;
            uint64_t src, dest;

            while (std::getline(file, line)) {
                if (line[0] == '#')
                    continue;

                std::istringstream iss(line);
                if (iss >> src >> dest) {
                    fwd_counts[src]++;
                    bwd_counts[dest]++;
                }
            }
        }

        // Allocate vectors on heap
        auto _tmp_fwd_adj_list = std::make_unique<std::vector<uint64_t>[]>(get_rows_size());
        auto _tmp_bwd_adj_list = std::make_unique<std::vector<uint64_t>[]>(get_rows_size());

        // Pre-allocate space
        for (size_t i = 0; i < get_rows_size(); i++) {
            if (fwd_counts[i] > 0) {
                _tmp_fwd_adj_list[i].reserve(fwd_counts[i]);
            }
            if (bwd_counts[i] > 0) {
                _tmp_bwd_adj_list[i].reserve(bwd_counts[i]);
            }
        }

        // Second pass: Build adjacency lists
        std::ifstream file;
        CSVIngestionEngine::process_file(file_path, file);
        std::string line;
        uint64_t src, dest;

        while (std::getline(file, line)) {
            if (line[0] == '#')
                continue;

            std::istringstream iss(line);
            if (iss >> src >> dest) {
                _tmp_fwd_adj_list[src].push_back(dest);
                _tmp_bwd_adj_list[dest].push_back(src);
            }
        }
#ifdef MY_DEBUG
        MemoryDebugUtility::print_adj_list(_tmp_fwd_adj_list, get_max_id_value());
        MemoryDebugUtility::print_adj_list(_tmp_bwd_adj_list, get_max_id_value(), true);
#endif
        populate_fwd_adj_list(_tmp_fwd_adj_list, get_rows_size());
        populate_bwd_adj_list(_tmp_bwd_adj_list, get_rows_size());
    }

    uint64_t DataSourceTable::get_rows_size() const { return _max_id_value + 1; }

    void DataSourceTable::populate_store_with_temporary_data() {
        _fwd_adj_list = std::make_unique<AdjList[]>(5);
        _bwd_adj_list = std::make_unique<AdjList[]>(5);
        populate_sample_data(_fwd_adj_list, _bwd_adj_list);
    }

    void DataSourceTable::populate_fwd_adj_list(const std::unique_ptr<std::vector<uint64_t>[]> &_tmp_adj_list,
                                                uint64_t size) {
        _fwd_adj_list = std::make_unique<AdjList[]>(size);
        for (uint64_t src = 0; src < size; ++src) {
            const auto &nbrs = _tmp_adj_list[src];
            auto size = nbrs.size();
            AdjList list(size);
            for (auto i = 0; i < size; i++) {
                list._values[i] = nbrs[i];
            }
            _fwd_adj_list[src] = std::move(list);
        }
    }

    void DataSourceTable::populate_bwd_adj_list(const std::unique_ptr<std::vector<uint64_t>[]> &_tmp_adj_list,
                                                uint64_t size) {
        _bwd_adj_list = std::make_unique<AdjList[]>(size);
        for (uint64_t src = 0; src < size; ++src) {
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
        const auto &filepath = get_dataset_serialized_data_reading_path();
        if (!filepath) {
            return;
        }

        _fwd_adj_list = std::make_unique<AdjList[]>(get_rows_size());
        _bwd_adj_list = std::make_unique<AdjList[]>(get_rows_size());
        const SerializeDeserialize<uint64_t> engine{filepath, this};
        engine.deserialize();
#ifdef MY_DEBUG
        MemoryDebugUtility::print_adj_list(_fwd_adj_list, _max_id_value);
        MemoryDebugUtility::print_adj_list(_bwd_adj_list, _max_id_value, true);
#endif
    }

    void DataSourceTable::write_table_as_data_on_disk() const {
        if (is_serializing_mode_disabled()) {
            return;
        }
        const auto &filepath = get_dataset_serialized_data_writing_path();
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
