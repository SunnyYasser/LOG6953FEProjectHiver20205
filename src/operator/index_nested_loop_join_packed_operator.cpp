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

#if defined(DISABLE_PTR_ALIAS)
#if defined(ARENA_ALLOCATOR)
        State *__restrict__ input_state = _input_vector->_state;
        State *__restrict__ output_state = _output_vector->_state;
        const uint64_t *__restrict__ _ip_vector_values = _input_vector->_values;
        uint64_t *__restrict__ _op_vector_values = _output_vector->_values;
        uint32_t *__restrict__ _op_vector_rle = output_state->_rle;
#else
        State *__restrict__ input_state = _input_vector->_state.get();
        State *__restrict__ output_state = _output_vector->_state.get();
        const uint64_t *__restrict__ _ip_vector_values = _input_vector->_values;
        uint64_t *__restrict__ _op_vector_values = _output_vector->_values;
        uint32_t *__restrict__ _op_vector_rle = output_state->_rle.get();
#endif
#else
#if defined(ARENA_ALLOCATOR)
        State *input_state = _input_vector->_state;
        State *output_state = _output_vector->_state;
        const uint64_t *_ip_vector_values = _input_vector->_values;
        uint64_t *_op_vector_values = _output_vector->_values;
        uint32_t *_op_vector_rle = output_state->_rle;
#else
        State *input_state = _input_vector->_state.get();
        State *output_state = _output_vector->_state.get();
        const uint64_t *_ip_vector_values = _input_vector->_values;
        uint64_t *_op_vector_values = _output_vector->_values;
        uint32_t *_op_vector_rle = output_state->_rle.get();
#endif
#endif

        // read information from the rle window
        const int32_t _ip_vector_pos = input_state->_state_info._curr_start_pos;
        const int32_t _ip_vector_size = input_state->_state_info._size;
        auto &_op_vector_rle_size = output_state->_rle_size; // should be 1
        auto &_op_vector_size = output_state->_state_info._size = 0; // since it is a new iteration, size is 0
#ifdef REMOVE_MEMSET
        auto &_op_rle_start_pos = output_state->_rle_start_pos;
#endif
        int32_t _op_filled_idx = 0; // idx to see how many of the output elements are filled
        int32_t _ip_values_idx = 0; // idx to see how many of adj list elements of curr ip_vector value are filled
        auto prev_ip_vector_pos = _ip_vector_pos;

#ifdef REMOVE_MEMSET
        // when this function is called for the first time, we need to ensure that the first rle generated is also
        // packed correctly, since the ip_vector start pos can be non zero
        // Mark range from 1 to _ip_vector_pos as valid (implicitly zero)
        _op_vector_rle_size += _ip_vector_pos;
        _op_rle_start_pos = _ip_vector_pos == 0 ? 0 : _op_vector_rle_size;

#else
        // when this function is called for the first time, we need to ensure that the first rle generated is also
        // packed correctly, since the ip_vector start pos can be non zero
        memset(&_op_vector_rle[1], 0, _ip_vector_pos * sizeof(_op_vector_rle[0]));
        _op_vector_rle_size += _ip_vector_pos;
#endif

        for (auto idx = 0; idx < _ip_vector_size;) {
            const auto curr_ip_vector_pos = _ip_vector_pos + idx;
            const auto &current_adj_node = (*_adj_list)[_ip_vector_values[curr_ip_vector_pos]];
#ifdef DISABLE_PTR_ALIAS
            const uint64_t *__restrict__ new_values_to_be_filled = current_adj_node._values;
#else
            const auto &new_values_to_be_filled = current_adj_node._values;
#endif
            const auto new_values_size = current_adj_node._size;
            const auto remaining_space = State::MAX_VECTOR_SIZE - _op_filled_idx;
            const auto remaining_values = new_values_size - _ip_values_idx;
            const auto elements_to_copy = std::min(remaining_space, static_cast<int32_t>(remaining_values));

            // Use memcpy to copy elements
            std::memcpy(&_op_vector_values[_op_filled_idx], &new_values_to_be_filled[_ip_values_idx],
                        elements_to_copy * sizeof(_op_vector_values[0]));

            _op_filled_idx += elements_to_copy;
            _ip_values_idx += elements_to_copy;

            // Update RLE information
#ifdef REMOVE_MEMSET
            auto prev_rle_value = _op_vector_rle[_op_vector_rle_size - 1];
            if (_op_vector_rle_size == _op_rle_start_pos)
                prev_rle_value = 0;
            _op_vector_rle[_op_vector_rle_size] = prev_rle_value + elements_to_copy;
#else
            _op_vector_rle[_op_vector_rle_size] = _op_vector_rle[_op_vector_rle_size - 1] + elements_to_copy;
#endif
            _op_vector_rle_size++;
            _op_vector_size += elements_to_copy;

            // Branchless updates using arithmetic
            const bool is_chunk_complete = (_ip_values_idx >= new_values_size);
            idx += is_chunk_complete;
            _ip_values_idx *= !is_chunk_complete; // Reset to 0 if chunk is complete

            const bool should_process = (_op_filled_idx >= State::MAX_VECTOR_SIZE) | (idx >= _ip_vector_size);

            // Use multiplication instead of if for conditional execution
            if (should_process) {
                // Save state
                const auto window_start = prev_ip_vector_pos;
                const auto window_size = curr_ip_vector_pos - prev_ip_vector_pos + 1;

                _input_vector->_state->_state_info._curr_start_pos = window_start;
                _input_vector->_state->_state_info._size = window_size;

#ifdef MY_DEBUG
                _debug->log_vector(_input_vector, _output_vector, fn_name);
#endif

                get_next_operator()->execute();

                // Reset output vector state
                _output_vector->_state->_state_info._size = 0;
                _output_vector->_state->_rle_size = 1;
                _output_vector->_state->_state_info._curr_start_pos = 0;
                _op_filled_idx = 0;

                // Reset input vector state
                _input_vector->_state->_state_info._curr_start_pos = _ip_vector_pos;
                _input_vector->_state->_state_info._size = _ip_vector_size;

                // Update slice position using arithmetic instead of conditional
                prev_ip_vector_pos = curr_ip_vector_pos + (_ip_values_idx == 0);

                // Reset RLE alignment
#ifdef REMOVE_MEMSET
                _op_vector_rle_size += (_ip_vector_pos + idx);
                _op_rle_start_pos = _op_vector_rle_size;
#else
                const auto rle_reset_size = (_ip_vector_pos + idx) * sizeof(_op_vector_rle[0]);
                std::memset(&_op_vector_rle[1], 0, rle_reset_size);
                _op_vector_rle_size += (_ip_vector_pos + idx);
#endif
            }
        }

        _op_vector_rle_size =
                1; // reset rle size to default, so next time non_incremental () is called we get clean values
        _op_vector_size = 0;
#ifdef REMOVE_MEMSET
        _op_rle_start_pos = 0;
#endif
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
