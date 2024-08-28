#include <iostream>
#include <exception>
#include <cassert>
#include "include/scan_operator.hh"
#include "include/operator_utils.hh"

namespace SampleDB {
    Scan::Scan(const std::vector<std::pair<std::string, std::string> > &scan_attribute,
               std::shared_ptr<Operator> next_operator) : Operator(scan_attribute.front().first,
                                                                   {scan_attribute.front().second}, next_operator),
                                                          _attribute(scan_attribute.front().second),
                                                          _attribute_data({}) {
    }

    operator_type_t Scan::get_operator_type() const {
        return OP_SCAN;
    }

    /*
    This routine ingests in a data vector, and processes it in chunks of
    size CHUNK_SIZE (1024)
    */

    void Scan::execute() {
        const std::string fn_name = "Scan::execute()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());

        int32_t start_idx = 0, prev_end_idx = 0;
        while (start_idx < _attribute_data.size()) {
            int32_t end_idx = std::min(static_cast<int32_t>(_attribute_data.size()) - 1 - prev_end_idx,
                                       start_idx + static_cast<int32_t>(State::MAX_VECTOR_SIZE));
            auto _chunked_data = std::vector<int32_t>(begin(_attribute_data) + start_idx,
                                                      begin(_attribute_data) + end_idx + 1);

            // update output vector for this operator
            _output_vector = Vector(_chunked_data);

            // log updated output vector
            log_vector(_output_vector, operator_name, fn_name, get_table_name());

            // update output vector in context
            _context_memory->update_column_data(get_table_name(), _attribute, _output_vector);

            // increment next chunk starting idx
            start_idx = end_idx + 1;
            prev_end_idx  = end_idx;

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
    void Scan::init(std::shared_ptr<ContextMemory> context, std::shared_ptr<DataStore> datastore) {
        _context_memory = context;
        _context_memory->allocate_memory_for_column(get_table_name(), _attribute);
        _attribute_data = datastore->get_data_vector_for_column(get_table_name(), _attribute);
        get_next_operator()->init(context, datastore);
    }
}
