//
// Created by Sunny on 21-08-2024.
//

/*
* This represents a table in SQL
*/
#ifndef SAMPLE_DB_DATASOURCE_H
#define SAMPLE_DB_DATASOURCE_H

#include <vector>
#include <string>
#include <cstdint>
#include <unordered_map>


namespace SampleDB {
    class DataSourceTable {
    public:
        explicit DataSourceTable(const std::string &, const std::vector<std::string> &);

        DataSourceTable(const DataSourceTable &) = default;

        int32_t rows_size() const;

        int32_t columns_size() const;

        /*name*/
        std::string _name;

        /*raw data table*/
        std::vector<std::vector<int32_t> > _table;

        /*simple index*/
        std::unordered_map<int32_t, std::vector<int32_t> > _index;

        /*"a" -> 0th, "b" -> 1st etc..*/
        std::unordered_map<std::string, uint32_t> _column_name_to_index_map;

    private:
        void populate_index();

        //TODO- this is just a testing API, and will be removed
        void populate_store_with_temporary_data();

        void populate();

        int32_t _rows{};
        int32_t _columns{};
    };
}

#endif //SAMPLE_DB_DATASOURCE_H
