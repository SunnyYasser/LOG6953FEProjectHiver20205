#include "include/index_nested_loop_join_packed_operator.hh"

#include <cassert>
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
        _relation_type(relation_type), _input_attribute(input_attribute), _output_attribute(output_attribute),
        _original_ip_selection_mask(nullptr), _current_ip_selection_mask(nullptr), _output_selection_mask(nullptr) {
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
    static inline void update_rle(uint32_t *rle, int32_t &rle_size, const uint32_t elements_to_add,
                                  const int32_t rle_start_pos) {
#ifdef MY_DEBUG
        assert(rle_size <= State::MAX_VECTOR_SIZE);
#endif

        auto prev_rle_value = rle[rle_size - 1];
        if (rle_size == rle_start_pos) {
            prev_rle_value = 0;
        }
        rle[rle_size] = prev_rle_value + elements_to_add;
        rle_size++;
    }
#endif


    __attribute__((always_inline)) inline void
    IndexNestedLoopJoinPacked::process_data_chunk(State *input_state, State *output_state, int32_t window_start,
                                                  int32_t window_size, int32_t ip_vector_pos, int32_t ip_vector_size,
                                                  int32_t &op_filled_idx, uint32_t *op_vector_rle, int32_t curr_pos,
                                                  int32_t idx, const std::string &fn_name) {
        input_state->_state_info._curr_start_pos = window_start;
        input_state->_state_info._size = window_size;
        // Set all output values as valid
        SET_ALL_BITS(*_output_selection_mask);
        // Get a copy of the current ip bitmask, since we need to restore it after
        // the function stack returns
        auto working_ip_bitmask_copy = *_current_ip_selection_mask;
        RESET_BITMASK(State::MAX_VECTOR_SIZE, working_ip_bitmask_copy, *_current_ip_selection_mask);

#ifdef MY_DEBUG
        _debug->log_vector(_input_vector, _output_vector, fn_name);
#endif

        get_next_operator()->execute();

        // Reset output state
        output_state->_state_info._size = 0;
        output_state->_rle_size = 1;
        output_state->_state_info._curr_start_pos = 0;
        op_filled_idx = 0;

        // Reset input state
        input_state->_state_info._curr_start_pos = ip_vector_pos;
        input_state->_state_info._size = ip_vector_size;
        // reset input bitmask to original working ip bitmask
        RESET_BITMASK(State::MAX_VECTOR_SIZE, *_current_ip_selection_mask, working_ip_bitmask_copy);

#ifdef MEMSET_TO_SET_VECTOR_SLICE
        const auto rle_reset_size = (ip_vector_pos + idx) * sizeof(op_vector_rle[0]);
        std::memset(&op_vector_rle[1], 0, rle_reset_size);
        output_state->_rle_size += (ip_vector_pos + idx);
#else
        output_state->_rle_size += (ip_vector_pos + idx);
        output_state->_rle_start_pos = output_state->_rle_size;
#endif
    }

    __attribute__((always_inline)) inline void
    IndexNestedLoopJoinPacked::copy_adjacency_values(uint64_t *op_vector_values, const uint64_t *adj_values,
                                                     const int32_t op_filled_idx, const int32_t ip_values_idx,
                                                     const int32_t elements_to_copy) {
        if (elements_to_copy == 0)
            return;
        std::memcpy(&op_vector_values[op_filled_idx], &adj_values[ip_values_idx],
                    elements_to_copy * sizeof(op_vector_values[0]));
    }

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
#else
        State *__restrict__ input_state = _input_vector->_state.get();
        State *__restrict__ output_state = _output_vector->_state.get();
        const uint64_t *__restrict__ _ip_vector_values = _input_vector->_values;
        uint64_t *__restrict__ _op_vector_values = _output_vector->_values;
        uint32_t *__restrict__ _op_vector_rle = output_state->_rle.get();
#endif
#else
#if defined(VECTOR_STATE_ARENA_ALLOCATOR)
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

#ifdef MY_DEBUG
        assert(input_state != nullptr);
        assert(output_state != nullptr);
        assert(_ip_vector_values != nullptr);
        assert(_op_vector_values != nullptr);
        assert(_op_vector_rle != nullptr);
#endif

        // Initialize processing state
        const int32_t ip_vector_pos = input_state->_state_info._curr_start_pos;
        const int32_t ip_vector_size = input_state->_state_info._size;
        auto &op_vector_rle_size = output_state->_rle_size;
        auto &op_vector_size = output_state->_state_info._size = 0;
        const auto &adj_list_ptr = *_adj_list;

        // Initialize RLE
#ifdef MEMSET_TO_SET_VECTOR_SLICE
        std::memset(&_op_vector_rle[1], 0, ip_vector_pos * sizeof(_op_vector_rle[0]));
        op_vector_rle_size += ip_vector_pos;
#else
        auto &op_rle_start_pos = output_state->_rle_start_pos;
        op_vector_rle_size += ip_vector_pos;
        op_rle_start_pos = ip_vector_pos == 0 ? 0 : op_vector_rle_size;
#endif

        // Process input vector
        int32_t op_filled_idx = 0;
        int32_t ip_values_idx = 0;
        int32_t prev_ip_vector_pos = ip_vector_pos;
        int32_t curr_pos = 0;
        int32_t remaining_space = 0;
        int32_t remaining_values = 0;
        int32_t elements_to_copy = 0;
        int32_t window_size = 0;
        int32_t output_elems_produced = 0;
        bool is_chunk_complete = false;

        // Reset the working mask with the original mask values at the start of execution
        RESET_BITMASK(State::MAX_VECTOR_SIZE, *_current_ip_selection_mask, *_original_ip_selection_mask);

        // Get the active bit range from the bitmask - this works for both BIT_ARRAY_AS_FILTER and regular bitmask
        int32_t start_idx = GET_START_POS(*_current_ip_selection_mask);
        int32_t end_idx = GET_END_POS(*_current_ip_selection_mask);

        // Ensure indices are within valid range
        start_idx = std::max(start_idx, ip_vector_pos);
        end_idx = std::min(end_idx, ip_vector_pos + ip_vector_size - 1);

        // Update RLE for elements from 0 to start_idx-1 (if start_idx > 0)
#ifdef MEMSET_TO_SET_VECTOR_SLICE
        // In MEMSET_TO_SET_VECTOR_SLICE mode, set RLE values to 0 for the skipped range
        const auto rle_range_size = start_idx * sizeof(_op_vector_rle[0]);
        std::memset(&_op_vector_rle[op_vector_rle_size], 0, rle_range_size);
        op_vector_rle_size += start_idx;
#else
        // In non-MEMSET mode, we can directly update the RLE start position
        op_vector_rle_size += start_idx;
        op_rle_start_pos = op_vector_rle_size;
#endif

        // We need to store the idx of the first and last valid idx of the bitmask
        int32_t first_valid_idx = -1, last_valid_idx = -1;

        // Process only the active bit range
        for (auto idx = start_idx; idx <= end_idx;) {
            // curr_pos = ip_vector_pos + idx;
            curr_pos = idx;
            const auto &curr_adj_node = adj_list_ptr[_ip_vector_values[curr_pos]];
            output_elems_produced = curr_adj_node._size;
            remaining_space = State::MAX_VECTOR_SIZE - op_filled_idx;
            remaining_values = output_elems_produced - ip_values_idx;
            elements_to_copy = std::min(remaining_space, remaining_values);

            if (!TEST_BIT(*_current_ip_selection_mask, idx) || output_elems_produced == 0) {
                CLEAR_BIT(*_current_ip_selection_mask, idx);
                elements_to_copy = 0;
            }

            // Track first and last valid indices
            if (elements_to_copy > 0 && first_valid_idx == -1) {
                first_valid_idx = idx;
            }
            if (elements_to_copy > 0) {
                last_valid_idx = idx;
            }

            // Update positions in the bitmask
            if (elements_to_copy > 0) {
                SET_START_POS(*_current_ip_selection_mask, first_valid_idx);
                SET_END_POS(*_current_ip_selection_mask, last_valid_idx);
            }

            copy_adjacency_values(_op_vector_values, curr_adj_node._values, op_filled_idx, ip_values_idx,
                                  elements_to_copy);

            op_filled_idx += elements_to_copy;
            ip_values_idx += elements_to_copy;

            // Update RLE
#ifdef MEMSET_TO_SET_VECTOR_SLICE
            update_rle(_op_vector_rle, op_vector_rle_size, elements_to_copy);
#else
            update_rle(_op_vector_rle, op_vector_rle_size, elements_to_copy, op_rle_start_pos);
#endif
            op_vector_size += elements_to_copy;

            is_chunk_complete = (ip_values_idx >= output_elems_produced);
            idx += is_chunk_complete;
            ip_values_idx *= !is_chunk_complete;

            if ((op_filled_idx >= State::MAX_VECTOR_SIZE) || (idx >= ip_vector_size)) {
                window_size = curr_pos - prev_ip_vector_pos + 1;
                process_data_chunk(input_state, output_state, prev_ip_vector_pos, window_size, ip_vector_pos,
                                   ip_vector_size, op_filled_idx, _op_vector_rle, curr_pos, idx, fn_name);
                prev_ip_vector_pos = curr_pos + (ip_values_idx == 0);
            }
        }

        // After processing the active bit range, update RLE for remaining elements
        if (end_idx < ip_vector_pos + ip_vector_size) {
            // Calculate number of remaining elements
            int32_t remaining_elements = (ip_vector_pos + ip_vector_size) - end_idx;

#ifdef MEMSET_TO_SET_VECTOR_SLICE
            // In MEMSET_TO_SET_VECTOR_SLICE mode, set RLE values to 0 for the remaining range
            const auto rle_range_size = remaining_elements * sizeof(_op_vector_rle[0]);
            std::memset(&_op_vector_rle[op_vector_rle_size], 0, rle_range_size);
            op_vector_rle_size += remaining_elements;
#else
            // In non-MEMSET mode, we can directly update the RLE start position
            op_vector_rle_size += remaining_elements;
            op_rle_start_pos = op_vector_rle_size;
#endif
        }

        // Final cleanup
        op_vector_rle_size = 1;
        op_vector_size = 0;
#ifndef MEMSET_TO_SET_VECTOR_SLICE
        op_rle_start_pos = 0;
#endif
        // Always restore original selection mask at the end of execution
        input_state->_selection_mask = _original_ip_selection_mask;
    }

    void IndexNestedLoopJoinPacked::execute() {
#ifdef MY_DEBUG
        assert(_original_ip_selection_mask != nullptr);
        assert(_current_ip_selection_mask != nullptr);
        assert(_output_selection_mask != nullptr);
        assert(_input_vector != nullptr);
        assert(_output_vector != nullptr);
#endif
        execute_internal();
    }

    void IndexNestedLoopJoinPacked::init(const std::shared_ptr<ContextMemory> &context,
                                         const std::shared_ptr<DataStore> &datastore) {
#ifdef MY_DEBUG
        _debug->log_operator_debug_msg();
#endif
        context->allocate_memory_for_column(_output_attribute);
        _input_vector = context->read_vector_for_column(_input_attribute);
        _output_vector = context->read_vector_for_column(_output_attribute);
        _output_vector->allocate_rle();
        _output_vector->allocate_selection_bitmask();

        // Always create a unique pointer for the original mask
        _original_ip_selection_mask_uptr = std::make_unique<BitMask<State::MAX_VECTOR_SIZE>>();
        // Create a separate mask for working operations
        _working_ip_selection_mask_uptr = std::make_unique<BitMask<State::MAX_VECTOR_SIZE>>();
        _original_ip_selection_mask = _original_ip_selection_mask_uptr.get();
        _current_ip_selection_mask = _working_ip_selection_mask_uptr.get();

        // Store a copy of the original input selection mask
        RESET_BITMASK(State::MAX_VECTOR_SIZE, *_original_ip_selection_mask, *(_input_vector->_state->_selection_mask));
        // Initialize the working mask with the same values
        RESET_BITMASK(State::MAX_VECTOR_SIZE, *_current_ip_selection_mask, *_original_ip_selection_mask);

        _output_selection_mask = _output_vector->_state->_selection_mask;

        if (_is_join_index_fwd)
            _adj_list = &(datastore->get_fwd_adj_lists());
        else
            _adj_list = &(datastore->get_bwd_adj_lists());

        _adj_list_size = datastore->get_table_rows_size();
        get_next_operator()->init(context, datastore);
    }

    unsigned long IndexNestedLoopJoinPacked::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine