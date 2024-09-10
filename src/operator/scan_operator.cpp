#include "include/scan_operator.hh"
#include <cassert>
#include <chrono>
#include <cmath>
#include <numeric>
#include <unordered_set>
#include "include/operator_utils.hh"

namespace VFEngine {
    Scan::Scan(const std::string &table_name, const std::string &scan_attribute,
               const std::shared_ptr<Operator> &next_operator) :
        Operator(table_name, next_operator), _max_id_value(0), _attribute(scan_attribute) {}

    operator_type_t Scan::get_operator_type() const { return OP_SCAN; }

    /*
    This routine ingests in a data vector, and processes it in chunks of
    size CHUNK_SIZE (1024)
    */

    void Scan::execute() {
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

            std::vector<uint64_t> _chunked_data (end - start + 1);
            auto start_itr = _chunked_data.begin();
            std::advance(start_itr, start);
            auto end_itr = _chunked_data.begin();
            std::advance(end_itr, end + 1);

            std::iota(start_itr, end_itr, start);

            // update output vector for this operator
            _output_vector = Vector(_chunked_data);

            // log updated output vector
            log_vector(_output_vector, operator_name, fn_name, get_table_name());

            // update output vector in context
            _context_memory->update_column_data(_attribute, _output_vector);

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
        _context_memory = context;
        _context_memory->allocate_memory_for_column(_attribute, get_table_name());
        _max_id_value = datastore->get_max_id_value(_attribute);
        get_next_operator()->init(context, datastore);
    }
} // namespace VFEngine
