#include "include/sink_operator.hh"
#include <iostream>
#include <vector>
#include "include/operator_utils.hh"


namespace VFEngine {
    long Sink::total_row_size_if_materialized = 0;
    long Sink::total_column_size_if_materialized = 0;

    Sink::Sink(const std::unordered_map<std::string, SchemaType> &schema) :
        Operator(), _schema(schema), _unique_states({}) {}

    operator_type_t Sink::get_operator_type() const { return OP_SINK; }

    void Sink::execute() {
        update_total_row_size_if_materialized();
    }

    void Sink::update_total_row_size_if_materialized() const {

        long curr{1};
        for (const auto &state: _unique_states) {
            curr *= state->_size;
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
        for (const auto &[column, value]: _schema) {
            if (value == UNFLAT) {
                const auto &vector = _context_memory->read_vector_for_column(column, get_table_name());
                const auto &state = vector._state;

                bool add_to_unique_states = true;

                for (auto &unique_state: _unique_states) {
                    if (unique_state == state.get()) {
                        add_to_unique_states = false;
                    }
                }

                if (add_to_unique_states) {
                    _unique_states.push_back(state.get());
                }
            }
        }
    }

    long Sink::get_total_row_size_if_materialized() {
        return total_row_size_if_materialized;
    }


} // namespace VFEngine
