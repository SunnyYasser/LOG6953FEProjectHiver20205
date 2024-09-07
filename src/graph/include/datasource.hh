//
// Created by Sunny on 21-08-2024.
//

/*
 * This represents a table in SQL
 */
#ifndef VFENGINE_DATASOURCE_H
#define VFENGINE_DATASOURCE_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>


namespace VFEngine {
    class DataSourceTable {
    public:
        explicit DataSourceTable(const std::string &name, const std::vector<std::string> &columns);

        DataSourceTable(const DataSourceTable &) = default;

        int32_t rows_size() const;

        int32_t columns_size() const;

        void write_table_as_data_on_disk(const std::string &filepath) const;
        void read_table_from_data_on_disk(const std::string &filepath);

        const std::unordered_map<int32_t, std::vector<int32_t>> &get_fwd_adj_list() const;

        const std::unordered_map<int32_t, std::vector<int32_t>> &get_bwd_adj_list() const;

        /*name*/
        std::string _name;

        /*raw data table*/
        std::vector<std::vector<int32_t>> _table;

        /*simple adj_lists*/
        std::unordered_map<int32_t, std::vector<int32_t>> _fwd_adj_list;

        std::unordered_map<int32_t, std::vector<int32_t>> _bwd_adj_list;

        /*"a" -> 0th, "b" -> 1st etc..*/
        std::unordered_map<std::string, uint32_t> _column_name_to_index_map;

    private:
        // TODO- this is just a testing API, and will be removed
        void populate_store_with_temporary_data();

        void populate_store();

        void populate_adj_list();

        void populate_fwd_adj_list();

        void populate_bwd_adj_list();

        int32_t _rows{};
        int32_t _columns{};
    };
} // namespace VFEngine

#endif
