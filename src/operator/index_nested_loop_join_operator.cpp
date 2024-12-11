#include "include/index_nested_loop_join_operator.hh"
#include <algorithm>
#include <cmath>
#include "include/operator_utils.hh"


namespace VFEngine {
    IndexNestedLoopJoin::IndexNestedLoopJoin(const std::string &input_attribute, const std::string &output_attribute,
                                             const bool &is_join_index_fwd, const RelationType &relation_type,
                                             const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _input_vector(nullptr), _output_vector(nullptr), _is_join_index_fwd(is_join_index_fwd),
        _relation_type(relation_type), _input_attribute(input_attribute), _output_attribute(output_attribute) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t IndexNestedLoopJoin::get_operator_type() const { return OP_INLJ; }

    void IndexNestedLoopJoin::execute_in_chunks_incremental() {
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_incremental()";
        _exec_call_counter++;
        const auto _initial_in_vector_pos = _input_vector->_state->_state_info._pos;
        _input_vector->_state->_state_info._pos++;
        while (_input_vector->_state->_state_info._pos < _input_vector->_state->_state_info._size) {
            execute_internal(fn_name);
            _input_vector->_state->_state_info._pos++;
        }
        _input_vector->_state->_state_info._pos = _initial_in_vector_pos;
    }

    void IndexNestedLoopJoin::execute_in_chunks_non_incremental() {
        _exec_call_counter++;
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_non_incremental()";
        execute_internal(fn_name);
    }

    void IndexNestedLoopJoin::execute_internal(const std::string &fn_name) {
        const auto &_ip_vector_pos = _input_vector->_state->_state_info._pos;
        const auto &_ip_vector_values = _input_vector->_values;

        // first de-ref to get the actual unique_ptr, then get Adjlist[] type for particular element, then get access to
        // underlying raw uint64_t array
        const auto &_ip_vector_adj_list = (*_adj_list)[_ip_vector_values[_ip_vector_pos]];
        const auto &_ip_vector_adj_list_values = _ip_vector_adj_list._values;
        const auto &_ip_vector_adj_list_size = _ip_vector_adj_list._size;

        constexpr auto chunk_size = State::MAX_VECTOR_SIZE;

        const int32_t number_chunks = std::ceil(static_cast<double>(_ip_vector_adj_list_size) / chunk_size);
        std::size_t start = 0, end = 0;

        for (int32_t chunk = 1; chunk <= number_chunks; ++chunk) {
            start = (chunk - 1) * chunk_size;

            // Branch less calculation of end index to handle leftover elements
            // If (max_id - start) >= chunk_size, then end = start + chunk_size
            // Else, end = max_id (remaining elements are less than chunk size)

            end = start + chunk_size * ((_ip_vector_adj_list_size - start) >= chunk_size) +
                  (_ip_vector_adj_list_size - start) * ((_ip_vector_adj_list_size - start) < chunk_size);

            const size_t op_vector_size = end - start;

            for (size_t idx = 0; idx < op_vector_size; idx++) {
                _output_vector->_values[idx] = _ip_vector_adj_list_values[start + idx];
            }

            _output_vector->_state->_state_info._size = static_cast<int32_t>(op_vector_size);
            _output_vector->_state->_state_info._pos = -1;

#ifdef MY_DEBUG
            // log updated output vector
            _debug->log_vector(_input_vector, _output_vector, fn_name);
#endif
            // call next operator
            get_next_operator()->execute();
        }
    }


    void IndexNestedLoopJoin::execute() {
        if (_input_vector->_state->_state_info._pos == -1)
            execute_in_chunks_incremental();
        else
            execute_in_chunks_non_incremental();
    }

    void IndexNestedLoopJoin::init(const std::shared_ptr<ContextMemory> &context,
                                   const std::shared_ptr<DataStore> &datastore) {
#ifdef MY_DEBUG
        _debug->log_operator_debug_msg();
#endif
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

    unsigned long IndexNestedLoopJoin::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
