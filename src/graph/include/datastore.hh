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

        explicit DataStore(const std::vector<std::string> &,
                           const std::unordered_map<std::string, std::vector<std::string>> &,
                           const std::unordered_map<std::string, std::string> &);

        DataStore(const DataStore &) = delete;

        DataStore(DataStore &&) = delete;

    public:
        [[nodiscard]] std::vector<uint64_t> get_data_vector_for_column_index(const uint32_t &column_idx,
                                                                             const std::string &table = "R") const;

        [[nodiscard]] std::vector<uint64_t> get_data_vector_for_column(const std::string &column,
                                                                       const std::string &table = "R") const;

        [[nodiscard]] int32_t get_table_columns_size(const std::string &table = "R") const;

        [[nodiscard]] int32_t get_table_rows_size(const std::string &table = "R") const;

        [[nodiscard]] const std::unordered_map<uint64_t, std::vector<uint64_t>> &
        get_fwd_adj_list(const std::string & = "R") const;

        [[nodiscard]] const std::unordered_map<uint64_t, std::vector<uint64_t>> &
        get_bwd_adj_list(const std::string & = "R") const;

        [[nodiscard]] uint64_t get_max_id_value(const std::string &column, const std::string &table = "R") const;


    private:
        uint32_t get_column_idx(const std::string &, const std::string &) const;

        void populate_tables();

        std::vector<std::string> _table_names;
        std::unordered_map<std::string, std::vector<std::string>> _table_to_column_map;
        std::unordered_map<std::string, std::shared_ptr<DataSourceTable>> _table_map;
        const std::unordered_map<std::string, std::string> _column_alias_map;
    };
} // namespace VFEngine

#endif
