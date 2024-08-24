#include <iostream>
#include <algorithm>
#include "include/index_nested_loop_join_operator.hh"
#include "include/operator_utils.hh"



namespace SampleDB
{
    IndexNestedLoopJoin::IndexNestedLoopJoin(const std::vector<std::string>& columns, std::shared_ptr <Operator> next_operator) : Operator(columns, next_operator), _input_attribute (columns.front()), _output_attribute (columns.back())
    {}

	operator_type_t IndexNestedLoopJoin::get_operator_type () const
    {
        return OP_INLJ;
    }

    void IndexNestedLoopJoin::execute_in_chunks()
    {
        const std::string fn_name = "INLJ::execute()";
        const std::string operator_name = get_operator_name_as_string (get_operator_type (), get_uuid ());

        auto &data = _input_vector.get_data_vector();

        while (_input_vector.get_pos() < _input_vector.get_size())
        {
            if (_input_vector.get_pos() == -1)
            {
                _input_vector.increment_pos();
            }

            const auto& _data_idx = _input_vector.get_pos();

            const auto& newdata_values = _datastore->get_values_from_index(data[_data_idx]);

            int start_idx = 0;
            std::vector<int32_t> _chunked_data;

            while (start_idx < newdata_values.size())
            {
                int end_idx = std::min({newdata_values.size(), start_idx + State::MAX_VECTOR_SIZE});

                auto start_itr = newdata_values.begin();
                std::advance(start_itr, start_idx);

                auto end_itr = newdata_values.begin();
                std::advance(end_itr, end_idx);

                //TODO: copy happening here, must come up with a better way for slicing
                _chunked_data = std::vector<int32_t>(start_itr, end_itr);

                _output_vector = Vector (_chunked_data);
                log_vector (_output_vector, operator_name, fn_name);

                _context_memory->update_operator_data(_output_attribute, _output_vector);

                start_idx = end_idx + 1;
            }


            _input_vector.increment_pos();
        }
    }

    void IndexNestedLoopJoin::execute()
    {
        _input_vector = _context_memory->read_vector_for_attribute (_input_attribute);
        execute_in_chunks();
        get_next_operator()->execute();
    }


    void IndexNestedLoopJoin::debug()
    {
        log_operator_debug_msg (this);
        get_next_operator()->debug();
    }

    void IndexNestedLoopJoin::init(std::shared_ptr <ContextMemory> context, std::shared_ptr <DataStore> datastore)
    {

        _context_memory = context;
        _context_memory->allocate_memory_for_operator(_output_attribute);
		_input_vector = context->read_vector_for_attribute (_input_attribute);
		_datastore = datastore;
        get_next_operator()->init(context, datastore);
    }

}