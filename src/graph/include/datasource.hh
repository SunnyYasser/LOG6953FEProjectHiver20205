//
// Created by Sunny on 21-08-2024.
//

/*
 * This represents a table in SQL
 */
#ifndef VFENGINE_DATASOURCE_H
#define VFENGINE_DATASOURCE_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "adjlist.hh"


namespace VFEngine {
    class DataSourceTable {
    public:
        explicit DataSourceTable(const std::string &name, const std::vector<std::string> &columns);
        DataSourceTable(const DataSourceTable &) = delete;
        uint64_t get_rows_size() const;
        void populate_datasource();
        void write_table_as_data_on_disk() const;
        void read_table_from_data_on_disk();
        uint64_t get_max_id_value() const;

        const std::unique_ptr<AdjList[]> &get_fwd_adj_list() const;
        const std::unique_ptr<AdjList[]> &get_bwd_adj_list() const;

        /*name*/
        std::string _name;
        // std::vector<std::vector<uint64_t>> _table;
        std::unique_ptr<AdjList[]> _fwd_adj_list;
        std::unique_ptr<AdjList[]> _bwd_adj_list;
        /*"a" -> 0th, "b" -> 1st etc..*/
        std::unordered_map<std::string, uint32_t> _column_name_to_index_map;

    private:
        void populate_store_with_temporary_data();

        void populate_csv_store();
        void populate_fwd_adj_list(const std::vector<std::vector<uint64_t>> &);
        void populate_bwd_adj_list(const std::vector<std::vector<uint64_t>> &);
        void populate_max_id_value();
        uint64_t _max_id_value{};
    };
} // namespace VFEngine

#endif
