#ifndef VFENGINE_DATASTORE_HH
#define VFENGINE_DATASTORE_HH

#include <cstdint>
#include <datasource.hh>
#include <memory>
#include <unordered_map>
#include <vector>
#include "graph.hh"

/*
This stores in memory the tables generated after graph
finished parsing the input file

Will send an object of this class to Pipeline to help
setup stuff during operator init ()
*/

namespace VFEngine {
    class DataStore {
    public:
        DataStore() = delete;

        explicit DataStore(const std::vector<std::string> &columns,
                           const std::unordered_map<std::string, std::string> &column_alias_map);

        DataStore(const DataStore &) = delete;

        DataStore(DataStore &&) = delete;

        [[nodiscard]] uint64_t get_table_rows_size() const;

        [[nodiscard]] const std::unique_ptr<AdjList[]> &get_fwd_adj_list() const;

        [[nodiscard]] const std::unique_ptr<AdjList[]> &get_bwd_adj_list() const;

        [[nodiscard]] uint64_t get_max_id_value() const;


    private:
        void populate_table();

        std::vector<std::string> _columns;
        std::shared_ptr<DataSourceTable> _table;
        const std::unordered_map<std::string, std::string> _column_alias_map;
    };
} // namespace VFEngine

#endif
