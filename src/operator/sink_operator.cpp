#include "include/sink_operator.hh"
#include <iostream>
#include <vector>
#include "include/operator_utils.hh"


namespace SampleDB {
    long Sink::total_row_size_if_materialized = 0;
    long Sink::total_column_size_if_materialized = 0;

    Sink::Sink(const std::shared_ptr<Schema> &schema) : Operator(schema) {}

    operator_type_t Sink::get_operator_type() const { return OP_SINK; }

    void Sink::execute() {

        const auto &schema_map = _schema->_schema_map;

        for (const auto &[column, value]: schema_map) {
            if (value == UNFLAT) {
                const auto &vector = _context_memory->read_vector_for_column(column, get_table_name());
                const auto &state = vector.get_state();
                _unique_states.insert(state);
            }
        }

        update_total_row_size_if_materialized();

        std::cout << "For Sink Operator " << get_operator_name_as_string(get_operator_type(), get_uuid()) << "\n";
        std::cout << "Total size output if materialized= " << total_row_size_if_materialized << "\n";
    }


    void Sink::update_total_row_size_if_materialized() const {

        long curr{1};
        for (const auto &state: _unique_states) {
            curr *= state->get_size();
        }

        total_row_size_if_materialized += curr;
    }

    void Sink::update_total_column_size_if_materialized() {}

    void Sink::debug() { log_operator_debug_msg(this); }

    /*
     * We only need to create a read the data of given column (attribute)
     * Output vector is not required for sink operator
     */
    void Sink::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        _context_memory = context;
        _datastore = datastore;
    }
} // namespace SampleDB
