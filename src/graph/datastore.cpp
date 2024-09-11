#include "include/datastore.hh"

#include <limits>
#include <memory>

namespace VFEngine {
    DataStore::DataStore(const std::vector<std::string> &columns,
                         const std::unordered_map<std::string, std::string> &column_alias_map) :
        _columns(columns), _column_alias_map(column_alias_map) {
        populate_table();
    }

    void DataStore::populate_table() { _table = std::make_shared<DataSourceTable>("R", _columns); }


    int32_t DataStore::get_table_columns_size() const { return _table->columns_size(); }

    int32_t DataStore::get_table_rows_size() const { return _table->rows_size(); }

    const std::vector<std::vector<uint64_t>> &DataStore::get_fwd_adj_list() const { return _table->get_fwd_adj_list(); }

    const std::vector<std::vector<uint64_t>> &DataStore::get_bwd_adj_list() const { return _table->get_bwd_adj_list(); }

    uint64_t DataStore::get_max_id_value() const { return _table->get_max_id_value(); }

} // namespace VFEngine
