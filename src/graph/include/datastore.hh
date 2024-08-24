#ifndef SAMPLE_DB_DATASTORE_HH
#define SAMPLE_DB_DATASTORE_HH

#include <vector>
#include <unordered_map>
#include <cstdint>
#include <datasource.hh>

#include "graph.hh"

/*
This stores in memory the tables generated after graph
finished parsing the input file

Will send an object of this class to Pipeline to help
setup stuff during operator init ()
*/

namespace SampleDB
{
    class DataStore
    {
    public:
        DataStore() = delete;
        explicit DataStore(const std::vector<std::string> &);
        DataStore(const DataStore &) = delete;
        DataStore(DataStore &&) = delete;

    public:
        [[nodiscard]] std::vector<int32_t> get_data_vector_for_column_index(const std::string&, const uint32_t);
        [[nodiscard]] std::vector<int32_t> get_data_vector_for_column(const std::string&, const std::string &);
        [[nodiscard]] std::vector<int32_t> get_values_from_index (const std::string&, const int32_t);

    private:
		uint32_t get_column_idx (const std::string&, const std::string&);
        std::unordered_map<std::string, DataSourceTable> _table_map;
        std::vector <std::string> _table_names;

    };
}

#endif