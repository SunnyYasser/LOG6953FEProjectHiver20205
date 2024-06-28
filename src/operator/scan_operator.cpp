#include <iostream>
#include <exception>
#include <cassert>
#include "include/scan_operator.hh"
#include "include/operator_utils.hh"

namespace SampleDB
{
    Scan::Scan(const std::vector<std::string>& columns, std::shared_ptr<Operator> next_operator) : Operator(columns, next_operator), _attribute (columns.front()), _attribute_data ({})
    {}

    operator_type_t Scan::get_operator_type () const
    {
        return OP_SCAN;
    }

    /*
    This routine ingests in a data vector, and processes it in chunks of
    size CHUNK_SIZE (1024)
    */

    void Scan::execute()
    {
        int start_idx = 0, end_idx;
        while (start_idx < _attribute_data.size())
        {
            int end_idx = std::min (_attribute_data.size() - 1, start_idx + State::MAX_VECTOR_SIZE);
            auto _chunked_data = std::vector<int32_t>(begin(_attribute_data) + start_idx, begin(_attribute_data) + end_idx + 1);

            // update output vector for this operator
            _output_vector = Vector (_chunked_data);

            // log updated output vector
            log_vector(_output_vector, get_operator_name_as_string (get_operator_type (), get_uuid ()), "Scan::execute()");

            // update output vector in context
            _context_memory->update_operator_data(_attribute, _output_vector);

            // increment next chunk starting idx
            start_idx = end_idx + 1;

            // call next operator
            get_next_operator ()->execute();

        }
    }

    void Scan::debug()
    {
        log_operator_debug_msg (this);
        get_next_operator()->debug();
    }

    /*
    * We only need to create a copy of the data of given column (attribute)
    * Input vector is not required for scan operator
    */
    void Scan::init(std::shared_ptr <ContextMemory> context, std::shared_ptr <DataStore> datastore)
    {
        _context_memory = context;
        _context_memory->allocate_memory_for_operator(_attribute);
        _attribute_data = datastore->get_data_vector_for_column (_attribute);
        get_next_operator()->init(context, datastore);
    }

}
