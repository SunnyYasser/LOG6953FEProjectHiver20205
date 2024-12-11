#include "include/index_nested_loop_join_packed_operator.hh"
#include <cmath>
#include <cstring>

#include "include/operator_utils.hh"


namespace VFEngine {
    IndexNestedLoopJoinPacked::IndexNestedLoopJoinPacked(const std::string &input_attribute,
                                                         const std::string &output_attribute,
                                                         const bool &is_join_index_fwd,
                                                         const RelationType &relation_type,
                                                         const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _input_vector(nullptr), _output_vector(nullptr), _is_join_index_fwd(is_join_index_fwd),
        _relation_type(relation_type), _input_attribute(input_attribute), _output_attribute(output_attribute) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t IndexNestedLoopJoinPacked::get_operator_type() const { return OP_INLJ_PACKED; }

    void IndexNestedLoopJoinPacked::execute_internal() {
        _exec_call_counter++;
        const std::string fn_name = "IndexNestedLoopJoinPacked::execute_internal()";

        // read information from the rle window
        const int32_t _ip_vector_pos = _input_vector->_state->_state_info._curr_start_pos;
        const int32_t _ip_vector_size = _input_vector->_state->_state_info._size;
        const auto &_ip_vector_values = _input_vector->_values;

        auto &_op_vector_rle = _output_vector->_state->_rle; // should have _rle[0] = 1
        auto &_op_vector_rle_size = _output_vector->_state->_rle_size; // should be 1
        auto &_op_vector_values = _output_vector->_values; // should be garbage of size 1024
        auto &_op_vector_size = _output_vector->_state->_state_info._size = 0; // since it is a new iteration, size is 0

        int32_t _op_filled_idx = 0; // idx to see how many of the output elements are filled
        int32_t _ip_values_idx = 0; // idx to see how many of adj list elements of curr ip_vector value are filled
        auto prev_ip_vector_pos = _ip_vector_pos;
        auto curr_ip_vector_pos = _ip_vector_pos;


        // when this function is called for the first time, we need to ensure that the first rle generated is also
        // packed correctly, since the ip_vector start pos can be non zero
        memset(&_op_vector_rle[1], 0, _ip_vector_pos * sizeof(_op_vector_rle[0]));
        _op_vector_rle_size += _ip_vector_pos;


        for (auto idx = 0; idx < _ip_vector_size;) {
            curr_ip_vector_pos = _ip_vector_pos + idx;
            const auto &new_values_to_be_filled = (*_adj_list)[_ip_vector_values[curr_ip_vector_pos]]._values;
            const auto &new_values_size = (*_adj_list)[_ip_vector_values[curr_ip_vector_pos]]._size;

            const auto prev_op_filled_idx = _op_filled_idx;

            for (; _op_filled_idx < State::MAX_VECTOR_SIZE and _ip_values_idx < new_values_size;
                 _ip_values_idx++, _op_filled_idx++) {
                _op_vector_values[_op_filled_idx] = new_values_to_be_filled[_ip_values_idx];
            }

            const int32_t op_elems_filled_size =
                    _op_filled_idx - prev_op_filled_idx; // we don't add 1 here since _op_filled_idx is already 1 ahead
                                                         // of actual value, because of the for loop

            _op_vector_rle[_op_vector_rle_size] = _op_vector_rle[_op_vector_rle_size - 1] + op_elems_filled_size;
            _op_vector_rle_size++;
            _op_vector_size += op_elems_filled_size;

            if (_ip_values_idx >= new_values_size) {
                // we have completed processing current ip_vector value, now increment ip_vector_pos and start from 0
                _ip_values_idx = 0;
                idx++;
            }

            if (_op_filled_idx >= State::MAX_VECTOR_SIZE or
                idx >= _ip_vector_size) { // Either we have completely filled output vector or we have
                                          // finished iterating input vector

                // We set the window starting index (_curr_start_pos) and size (_size) for packed implementation.
                // We need to store the previous _curr_start_pos and _size, so when control flow returns after
                // recursion, we have the exactly correct previous values.  It should be kept in mind that the window
                // information will be modified by each join operator when it produces its own output tuples after
                // consuming a set of input tuples. However, the stack ensures each operator will have the correct value

                _input_vector->_state->_state_info._curr_start_pos = prev_ip_vector_pos;
                _input_vector->_state->_state_info._size = curr_ip_vector_pos - prev_ip_vector_pos + 1;

#ifdef MY_DEBUG
                // log updated output vector
                _debug->log_vector(_input_vector, _output_vector, fn_name);
#endif

                get_next_operator()->execute();

                // reset output vector
                _output_vector->_state->_state_info._size = 0;
                _output_vector->_state->_rle_size = 1;
                _output_vector->_state->_state_info._curr_start_pos = 0;

                // output vector will be filled again from start
                _op_filled_idx = 0;

                // reset input_vector rle information
                _input_vector->_state->_state_info._curr_start_pos = _ip_vector_pos;
                _input_vector->_state->_state_info._size = _ip_vector_size;

                // update start of new slice, if older slice is complete, then increment by 1
                prev_ip_vector_pos = curr_ip_vector_pos + (_ip_values_idx == 0);

                // align rle each time to correctly slide across the parent vector chunk start and size
                memset(&_op_vector_rle[1], 0, (_ip_vector_pos + idx) * sizeof(_op_vector_rle[0]));
                _op_vector_rle_size += (_ip_vector_pos + idx);
            }
        }

        _op_vector_rle_size =
                1; // reset rle size to default, so next time non_incremental () is called we get clean values
        _op_vector_size = 0;
    }


    void IndexNestedLoopJoinPacked::execute() { execute_internal(); }

    void IndexNestedLoopJoinPacked::init(const std::shared_ptr<ContextMemory> &context,
                                         const std::shared_ptr<DataStore> &datastore) {
#ifdef MY_DEBUG
        _debug->log_operator_debug_msg();
#endif
        context->allocate_memory_for_column(_output_attribute);
        _input_vector = context->read_vector_for_column(_input_attribute);
        _output_vector = context->read_vector_for_column(_output_attribute);
        _output_vector->allocate_rle();

        if (_is_join_index_fwd)
            _adj_list = &(datastore->get_fwd_adj_lists());
        else
            _adj_list = &(datastore->get_bwd_adj_lists());

        _adj_list_size = datastore->get_table_rows_size();
        get_next_operator()->init(context, datastore);
    }

    unsigned long IndexNestedLoopJoinPacked::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
