#ifndef SAMPLE_DB_DATASTORE_HH
#define SAMPLE_DB_DATASTORE_HH

#include <vector>
#include <unordered_map>
#include <cstdint>
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
        //TODO- this is just a testing API, and will be removed
        void populate_store_with_temporary_data();
        const std::vector<int32_t> get_data_vector_for_column_index(const uint32_t) const;
        const std::vector<int32_t> get_data_vector_for_column(const std::string &) const;
        const std::vector<int32_t> get_values_from_index (const int32_t) const;

    private:
        void populate_index();

    private:
        /*raw data table*/
        std::vector<std::vector<int32_t>> _datastore;

        /*column_name : value : vector of other values in the same tuple*
        eg -
        "a"-> "b" {[0,1,2], [0,1,3]}
        "a" -> [0] = [[1,2], [1, 3]]
        std::unordered_map<std::string, std::unordered_map<int32_t, std::vector<std::vector<int32_t>>>> _index;
        */

        /*
            The above is for tables having more than two attributes, in our case
            we can live with {value, vector_of_incident_values}

         */
        std::unordered_map<int32_t, std::vector<int32_t>> _index;

        /*"a" -> 0th, "b" -> 1st etc..*/
        std::unordered_map<std::string, uint32_t> _column_name_to_index_map;

        /*columns */
        const std::vector<std::string> _columns;
    };
}

#endif