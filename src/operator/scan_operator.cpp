#include "include/scan_operator.hh"
#include <cassert>
#include <chrono>
#include <cmath>
#include <numeric>
#include "include/operator_utils.hh"

namespace VFEngine {
    Scan::Scan(const std::string &scan_attribute, const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _output_vector(nullptr), _max_id_value(0), _attribute(scan_attribute) {}

    operator_type_t Scan::get_operator_type() const { return OP_SCAN; }

    /*
    This routine ingests in a data vector, and processes it in chunks of
    size CHUNK_SIZE (1024)
    */

    void Scan::execute() {
        _exec_call_counter++;
        const std::string fn_name = "Scan::execute()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());

        constexpr auto chunk_size = State::MAX_VECTOR_SIZE;
        size_t number_chunks = std::ceil(static_cast<double>(_max_id_value) / chunk_size);
        size_t start = 0, end = 0;

        for (size_t chunk = 1; chunk <= number_chunks; ++chunk) {
            start = (chunk - 1) * chunk_size;

            // Branch less calculation of end index to handle leftover elements
            // If (max_id - start) >= chunk_size, then end = start + chunk_size
            // Else, end = max_id (remaining elements are less than chunk size)

            end = start + chunk_size * ((_max_id_value - start) >= chunk_size) +
                  (_max_id_value - start) * ((_max_id_value - start) < chunk_size);

            const size_t _curr_chunk_size = end - start;

            for (size_t i = 0; i < _curr_chunk_size; ++i) {
                _output_vector->_values[i] = start + i;
            }

            _output_vector->_state->_size = static_cast<int32_t>(_curr_chunk_size);
            _output_vector->_state->_pos = -1;

            // log updated output vector
            log_vector(_output_vector, operator_name, fn_name);

            // call next operator
            get_next_operator()->execute();
        }
    }

    void Scan::debug() {
        log_operator_debug_msg(this);
        get_next_operator()->debug();
    }

    /*
     * We only need to create a copy of the data of given column (attribute)
     * Input vector is not required for scan operator
     */
    void Scan::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        context->allocate_memory_for_column(_attribute);
        _output_vector = context->read_vector_for_column(_attribute);
        _max_id_value = datastore->get_max_id_value();
        get_next_operator()->init(context, datastore);
    }

    ulong Scan::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
