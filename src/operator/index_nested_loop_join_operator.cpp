#include "include/index_nested_loop_join_operator.hh"
#include <algorithm>
#include <cmath>
#include <iostream>
#include "include/operator_utils.hh"


namespace VFEngine {
    IndexNestedLoopJoin::IndexNestedLoopJoin(const std::string &input_attribute, const std::string &output_attribute,
                                             const bool &is_join_index_fwd, const RelationType &relation_type,
                                             const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _input_vector(nullptr), _output_vector(nullptr), _is_join_index_fwd(is_join_index_fwd),
        _relation_type(relation_type), _input_attribute(input_attribute), _output_attribute(output_attribute) {}

    operator_type_t IndexNestedLoopJoin::get_operator_type() const { return OP_INLJ; }

    void IndexNestedLoopJoin::execute_in_chunks_incremental() {
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_incremental()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());
        _exec_call_counter++;
        _input_vector->_state->_pos++;
        while (_input_vector->_state->_pos < _input_vector->_state->_size) {
            execute_internal(fn_name, operator_name);
            _input_vector->_state->_pos++;
        }
    }

    void IndexNestedLoopJoin::execute_in_chunks_non_incremental() {
        _exec_call_counter++;
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_non_incremental()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());
        execute_internal(fn_name, operator_name);
    }

    void IndexNestedLoopJoin::execute_internal(const std::string &fn_name, const std::string &operator_name) {
        const auto &_data_idx = _input_vector->_state->_pos;
        const auto &data = _input_vector->_values;
        // first de-ref to get the actual unique_ptr, then get Adjlist[] type for particular element, then get access to
        // underlying raw uint64_t array
        const auto &newdata_values_adj_list = (*_adj_list)[data[_data_idx]];
        const auto &newdata_values = newdata_values_adj_list._values;
        const auto &newdata_values_size = newdata_values_adj_list._size;
        constexpr auto chunk_size = State::MAX_VECTOR_SIZE;

        const int32_t number_chunks = std::ceil(static_cast<double>(newdata_values_size) / chunk_size);
        std::size_t start = 0, end = 0;

        for (int32_t chunk = 1; chunk <= number_chunks; ++chunk) {
            start = (chunk - 1) * chunk_size;

            // Branch less calculation of end index to handle leftover elements
            // If (max_id - start) >= chunk_size, then end = start + chunk_size
            // Else, end = max_id (remaining elements are less than chunk size)

            end = start + chunk_size * ((newdata_values_size - start) >= chunk_size) +
                  (newdata_values_size - start) * ((newdata_values_size - start) < chunk_size);

            size_t idx = 0;
            size_t op_vector_size = end - start;
            for (; idx <= op_vector_size; idx++) {
                _output_vector->_values[idx] = newdata_values[start + idx];
            }

            _output_vector->_state->_size = static_cast<int32_t>(op_vector_size);
            _output_vector->_state->_pos = -1;

            // log updated output vector
            log_vector(_input_vector, _output_vector, operator_name, fn_name);

            // call next operator
            get_next_operator()->execute();
        }
    }


    void IndexNestedLoopJoin::execute() {
        if (_input_vector->_state->_pos == -1)
            execute_in_chunks_incremental();
        else
            execute_in_chunks_non_incremental();
    }


    void IndexNestedLoopJoin::debug() {
        log_operator_debug_msg(this);
        get_next_operator()->debug();
    }

    void IndexNestedLoopJoin::init(const std::shared_ptr<ContextMemory> &context,
                                   const std::shared_ptr<DataStore> &datastore) {

        context->allocate_memory_for_column(_output_attribute);

        _input_vector = context->read_vector_for_column(_input_attribute);
        _output_vector = context->read_vector_for_column(_output_attribute);

        if (_is_join_index_fwd)
            _adj_list = &(datastore->get_fwd_adj_lists());
        else
            _adj_list = &(datastore->get_bwd_adj_lists());

        _adj_list_size = datastore->get_table_rows_size();

        get_next_operator()->init(context, datastore);
    }

    ulong IndexNestedLoopJoin::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
