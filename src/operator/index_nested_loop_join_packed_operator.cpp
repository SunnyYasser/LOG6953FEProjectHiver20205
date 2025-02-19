#include "include/index_nested_loop_join_packed_operator.hh"
#include <cmath>
#include <cstring>
#include "include/operator_utils.hh"

namespace VFEngine {
    // Constructor remains unchanged
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

#ifdef MEMSET_TO_SET_VECTOR_SLICE
    static inline void update_rle(uint32_t *rle, int32_t &rle_size, uint32_t elements_to_add) {
        rle[rle_size] = rle[rle_size - 1] + elements_to_add;
        rle_size++;
    }
#else
    static inline void update_rle(uint32_t *rle, int32_t &rle_size, uint32_t elements_to_add, int32_t rle_start_pos) {
        auto prev_rle_value = rle[rle_size - 1];
        if (rle_size == rle_start_pos) {
            prev_rle_value = 0;
        }
        rle[rle_size] = prev_rle_value + elements_to_add;
        rle_size++;
    }
#endif


    void IndexNestedLoopJoinPacked::execute_internal() {
        _exec_call_counter++;
        const std::string fn_name = "IndexNestedLoopJoinPacked::execute_internal()";

#if defined(STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS)
#if defined(VECTOR_STATE_ARENA_ALLOCATOR)
        State *__restrict__ input_state = _input_vector->_state;
        State *__restrict__ output_state = _output_vector->_state;
        const uint64_t *__restrict__ _ip_vector_values = _input_vector->_values;
        uint64_t *__restrict__ _op_vector_values = _output_vector->_values;
        uint32_t *__restrict__ _op_vector_rle = output_state->_rle;
        // BitMask<State::MAX_VECTOR_SIZE> &_ip_selection_mask = *(input_state->_selection_mask);
#else
        State *__restrict__ input_state = _input_vector->_state.get();
        State *__restrict__ output_state = _output_vector->_state.get();
        const uint64_t *__restrict__ _ip_vector_values = _input_vector->_values;
        uint64_t *__restrict__ _op_vector_values = _output_vector->_values;
        uint32_t *__restrict__ _op_vector_rle = output_state->_rle.get();
        // BitMask<State::MAX_VECTOR_SIZE> &_ip_selection_mask = *(input_state->_selection_mask);
#endif
#else
#if defined(VECTOR_STATE_ARENA_ALLOCATOR)
        State *input_state = _input_vector->_state;
        State *output_state = _output_vector->_state;
        const uint64_t *_ip_vector_values = _input_vector->_values;
        uint64_t *_op_vector_values = _output_vector->_values;
        uint32_t *_op_vector_rle = output_state->_rle;
        // BitMask<State::MAX_VECTOR_SIZE> &_ip_selection_mask = *(input_state->_selection_mask);
#else
        State *input_state = _input_vector->_state.get();
        State *output_state = _output_vector->_state.get();
        const uint64_t *_ip_vector_values = _input_vector->_values;
        uint64_t *_op_vector_values = _output_vector->_values;
        uint32_t *_op_vector_rle = output_state->_rle.get();
        // BitMask<State::MAX_VECTOR_SIZE> &_ip_selection_mask = *(input_state->_selection_mask);
#endif
#endif

        const int32_t _ip_vector_pos = input_state->_state_info._curr_start_pos;
        const int32_t _ip_vector_size = input_state->_state_info._size;
        auto &_op_vector_rle_size = output_state->_rle_size;
        auto &_op_vector_size = output_state->_state_info._size = 0;
        const auto &adj_list_ptr = *_adj_list;
        int32_t curr_ip_vector_pos = 0;
        ulong current_val = 0;
        int32_t _op_filled_idx = 0;
        int32_t _ip_values_idx = 0;
        auto prev_ip_vector_pos = _ip_vector_pos;
        uint64_t *new_values_to_be_filled = nullptr;
        int32_t new_values_size = 0;
        int32_t remaining_space = 0;
        int32_t remaining_values = 0;
        int32_t elements_to_copy = 0;
        bool is_chunk_complete = false;
        bool should_process = false;
        int32_t window_start = 0;
        int32_t window_size = 0;

#ifdef MEMSET_TO_SET_VECTOR_SLICE
        memset(&_op_vector_rle[1], 0, _ip_vector_pos * sizeof(_op_vector_rle[0]));
        _op_vector_rle_size += _ip_vector_pos;
#else
        auto &_op_rle_start_pos = output_state->_rle_start_pos;
        _op_vector_rle_size += _ip_vector_pos;
        _op_rle_start_pos = _ip_vector_pos == 0 ? 0 : _op_vector_rle_size;
#endif
        for (auto idx = 0; idx < _ip_vector_size;) {
            curr_ip_vector_pos = _ip_vector_pos + idx;
            current_val = _ip_vector_values[curr_ip_vector_pos];
            const auto &current_adj_node = adj_list_ptr[current_val];
#ifdef STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS
            const uint64_t *__restrict__ new_values_to_be_filled = current_adj_node._values;
#else
            new_values_to_be_filled = current_adj_node._values;
#endif
            new_values_size = current_adj_node._size;
            remaining_space = State::MAX_VECTOR_SIZE - _op_filled_idx;
            remaining_values = new_values_size - _ip_values_idx;
            elements_to_copy = std::min(remaining_space, static_cast<int32_t>(remaining_values));
            std::memcpy(&_op_vector_values[_op_filled_idx], &new_values_to_be_filled[_ip_values_idx],
                        elements_to_copy * sizeof(_op_vector_values[0]));

            _op_filled_idx += elements_to_copy;
            _ip_values_idx += elements_to_copy;

#ifdef MEMSET_TO_SET_VECTOR_SLICE
            update_rle(_op_vector_rle, _op_vector_rle_size, elements_to_copy);
#else
            update_rle(_op_vector_rle, _op_vector_rle_size, elements_to_copy, _op_rle_start_pos);
#endif
            _op_vector_size += elements_to_copy;
            is_chunk_complete = (_ip_values_idx >= new_values_size);
            idx += is_chunk_complete;
            _ip_values_idx *= !is_chunk_complete;
            should_process = (_op_filled_idx >= State::MAX_VECTOR_SIZE) | (idx >= _ip_vector_size);

            if (should_process) {
                window_start = prev_ip_vector_pos;
                window_size = curr_ip_vector_pos - prev_ip_vector_pos + 1;

                input_state->_state_info._curr_start_pos = window_start;
                input_state->_state_info._size = window_size;

#ifdef MY_DEBUG
                _debug->log_vector(_input_vector, _output_vector, fn_name);
#endif

                get_next_operator()->execute();

                output_state->_state_info._size = 0;
                output_state->_rle_size = 1;
                output_state->_state_info._curr_start_pos = 0;
                _op_filled_idx = 0;

                input_state->_state_info._curr_start_pos = _ip_vector_pos;
                input_state->_state_info._size = _ip_vector_size;

                prev_ip_vector_pos = curr_ip_vector_pos + (_ip_values_idx == 0);

#ifdef MEMSET_TO_SET_VECTOR_SLICE
                const auto rle_reset_size = (_ip_vector_pos + idx) * sizeof(_op_vector_rle[0]);
                std::memset(&_op_vector_rle[1], 0, rle_reset_size);
                _op_vector_rle_size += (_ip_vector_pos + idx);
#else
                _op_vector_rle_size += (_ip_vector_pos + idx);
                _op_rle_start_pos = _op_vector_rle_size;
#endif
            }
        }

        _op_vector_rle_size = 1;
        _op_vector_size = 0;
#ifdef MEMSET_TO_SET_VECTOR_SLICE
        // No additional cleanup needed
#else
        _op_rle_start_pos = 0;
#endif
    }

    // Rest of the implementation remains unchanged
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
