#include "include/sink_operator.hh"
#include <iostream>
#include <vector>
#include "include/operator_utils.hh"


namespace VFEngine {
    ulong Sink::total_row_size_if_materialized = 0;

    Sink::Sink(const std::vector<std::pair<std::string, SchemaType>> &schema) :
        Operator(), _schema(schema), _unique_states({}) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t Sink::get_operator_type() const { return OP_SINK; }

    void Sink::execute() {
        _exec_call_counter++;
        update_total_row_size_if_materialized();
    }

    void Sink::update_total_row_size_if_materialized() const {

        long curr{1};
        for (const auto &state: _unique_states) {
            curr *= state->_state_info._size;
        }

        total_row_size_if_materialized += curr;
    }

    void Sink::update_total_column_size_if_materialized() {}

    /*
     * We only need to create a read the data of given column (attribute)
     * Output vector is not required for sink operator
     */
    void Sink::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        for (const auto &[column, value]: _schema) {
            if (value == UNFLAT) {
                const auto vector = context->read_vector_for_column(column);
                const auto &state = vector->_state;

                bool add_to_unique_states = true;

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
                for (auto &unique_state: _unique_states) {
                    if (unique_state == state) {
                        add_to_unique_states = false;
                    }
                }

                if (add_to_unique_states) {
                    _unique_states.push_back(state);
                }
#else
                for (auto &unique_state: _unique_states) {
                    if (unique_state == state.get()) {
                        add_to_unique_states = false;
                    }
                }
                if (add_to_unique_states) {
                    _unique_states.push_back(state.get());
                }
#endif
            }
        }
    }

    ulong Sink::get_total_row_size_if_materialized() { return total_row_size_if_materialized; }
    ulong Sink::get_exec_call_counter() const { return _exec_call_counter; }


} // namespace VFEngine
