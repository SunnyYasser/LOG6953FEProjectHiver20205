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

    static inline void update_rle(uint32_t *rle, const uint32_t index, const int32_t start_idx, const uint32_t size) {
#ifdef MY_DEBUG
        assert(index >= 0 && index < State::MAX_VECTOR_SIZE);
#endif

        // Set RLE value directly at the index
        rle[index] = start_idx;
        rle[index + 1] = start_idx + size;
    }

    __attribute__((always_inline)) inline void IndexNestedLoopJoinPacked::process_data_chunk(
            const int32_t new_ip_selection_vector_start_pos, const int32_t new_ip_selection_vector_end_pos,
            const int32_t current_ip_vector_idx, const bool is_chunk_complete, int32_t &op_filled_idx,
            uint32_t *op_vector_rle, const std::string &fn_name) {
#ifdef MY_DEBUG
        assert(op_filled_idx <= State::MAX_VECTOR_SIZE);
        assert(op_vector_rle != nullptr);
        assert(current_ip_vector_idx <= State::MAX_VECTOR_SIZE);
#endif

        // Set all idx upto op_filled_idx as valid, rest as invalid
        CLEAR_ALL_BITS(*_output_selection_mask);
        SET_BITS_TILL_IDX(*_output_selection_mask, op_filled_idx - 1);
        SET_START_POS(*_output_selection_mask, 0);
        SET_END_POS(*_output_selection_mask, op_filled_idx - 1);

        // First get a copy of the current ip bitmask, since we need to restore it after
        // the function stack returns
        const auto working_ip_bitmask_copy = *_current_ip_selection_mask;

        // Only update the START position if this is a complete chunk - otherwise
        // keep the previous start position (which might be for a specific index we're
        // still processing)
        // if (is_chunk_complete) {
        //     SET_START_POS(*_current_ip_selection_mask, new_ip_selection_vector_start_pos);
        // }

        // Always update the end position
        SET_START_POS(*_current_ip_selection_mask, new_ip_selection_vector_start_pos);
        SET_END_POS(*_current_ip_selection_mask, new_ip_selection_vector_end_pos);

        // Set input vector's selection mask to updated current mask before calling next operator
        _input_vector->_state->_selection_mask = _current_ip_selection_mask;

#ifdef MY_DEBUG
        _debug->log_vector(_input_vector, _output_vector, fn_name);
#endif

        get_next_operator()->execute();

        // Reset input bitmask to original working ip bitmask, start and end pos are also restored
        RESET_BITMASK(State::MAX_VECTOR_SIZE, *_current_ip_selection_mask, working_ip_bitmask_copy);

        // We need to set all the values upto the current ip_vector_idx to be marked invalid
        CLEAR_ALL_BITS(*_current_ip_selection_mask);

        // Set invalid start/end positions by default
        SET_START_POS(*_current_ip_selection_mask, State::MAX_VECTOR_SIZE - 1);
        SET_END_POS(*_current_ip_selection_mask, 0);

        // Only if we're still processing this index (chunk not complete),
        // set this bit and update positions
        if (!is_chunk_complete) {
            SET_BIT(*_current_ip_selection_mask, current_ip_vector_idx);
            SET_START_POS(*_current_ip_selection_mask, current_ip_vector_idx);
            SET_END_POS(*_current_ip_selection_mask, current_ip_vector_idx);
        }

        // Finally clean the output vector rle for new batch
        std::memset(op_vector_rle, 0, State::MAX_VECTOR_SIZE * sizeof(uint32_t));
        op_filled_idx = 0;
    }

    __attribute__((always_inline)) inline void
    IndexNestedLoopJoinPacked::copy_adjacency_values(uint64_t *op_vector_values, const uint64_t *adj_values,
                                                     const int32_t op_filled_idx, const int32_t ip_values_idx,
                                                     const int32_t elements_to_copy) {
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

        // Reset the working mask with the original mask values at the start of execution
        RESET_BITMASK(State::MAX_VECTOR_SIZE, *_original_ip_selection_mask, *(input_state->_selection_mask));
        RESET_BITMASK(State::MAX_VECTOR_SIZE, *_current_ip_selection_mask, *_original_ip_selection_mask);

        // Get the active bit range from the bitmask
        const int32_t start_idx = GET_START_POS(*_current_ip_selection_mask);
        const int32_t end_idx = GET_END_POS(*_current_ip_selection_mask);
        const auto &adj_list_ptr = *_adj_list;

#ifdef MY_DEBUG
        assert(start_idx <= end_idx);
        assert(start_idx >= 0 && start_idx < State::MAX_VECTOR_SIZE);
        assert(end_idx >= 0 && end_idx < State::MAX_VECTOR_SIZE);
#endif


        // Process input vector
        int32_t op_filled_idx = 0;
        int32_t ip_values_idx = 0;
        int32_t curr_pos = 0;
        int32_t output_elems_produced = 0;
        bool is_chunk_complete = false;

        // Initialize RLE[0] to 0
        std::memset(_op_vector_rle, 0, State::MAX_VECTOR_SIZE * sizeof(uint32_t));
        op_filled_idx = 0;

        // Process only the active bit range
        int32_t new_start_idx = -1, new_end_idx = -1;
        for (auto idx = start_idx; idx <= end_idx;) {

            // Skip if this index isn't valid in our selection mask
            if (!TEST_BIT(*_current_ip_selection_mask, idx)) {
                idx++;
                continue;
            }

            // Get adjacency list for current value
            const auto &curr_adj_node = adj_list_ptr[_ip_vector_values[idx]];
            output_elems_produced = curr_adj_node._size;

            // Skip if there are no elements to produce
            if (output_elems_produced == 0) {
                CLEAR_BIT(*_current_ip_selection_mask, idx);
                idx++;
                continue;
            }

            // Start idx to be updated once for each chunk
            if (new_start_idx == -1) {
                new_start_idx = idx;
            }

            // End idx to be updated always inside the loop
            new_end_idx = idx;

            // Calculate how many elements we can copy
            int32_t remaining_space = State::MAX_VECTOR_SIZE - op_filled_idx;
            int32_t remaining_values = output_elems_produced - ip_values_idx;
            const int32_t elements_to_copy = std::min(remaining_space, remaining_values);

            // Copy values to output
            copy_adjacency_values(_op_vector_values, curr_adj_node._values, op_filled_idx, ip_values_idx,
                                  elements_to_copy);


            // Update RLE directly at the index based on the element count
            if (elements_to_copy > 0) {
                update_rle(_op_vector_rle, idx, op_filled_idx, elements_to_copy);
            }

            // Update
            op_filled_idx += elements_to_copy;
            ip_values_idx += elements_to_copy;

            // Check if we've processed all elements for this index
            is_chunk_complete = (ip_values_idx >= output_elems_produced);

            // If output buffer is full or we're at the end of our input range, process the chunk
            if (op_filled_idx >= State::MAX_VECTOR_SIZE || (idx > end_idx)) {
                process_data_chunk(new_start_idx, // New start position for selection vector
                                   new_end_idx, // New end position for selection vector
                                   curr_pos, // Current index being processed
                                   is_chunk_complete, // Whether we completed this chunk
                                   op_filled_idx, // Number of items in output buffer
                                   _op_vector_rle, // Output RLE array
                                   fn_name // Function name for debug
                );
                // prev_start_idx = idx + is_chunk_complete;
                new_start_idx = -1;
            }

            // Move to next index if chunk is complete, otherwise keep working on this index
            if (is_chunk_complete) {
                idx++;
                ip_values_idx = 0;
            }
        }

        // Add this check after the loop to process any remaining elements
        if (op_filled_idx > 0) {
            // We have elements in the output buffer that need to be processed
            process_data_chunk(new_start_idx, new_end_idx,
                               end_idx, // Use end_idx as the current position
                               true, // Mark as complete
                               op_filled_idx, _op_vector_rle, fn_name);
        }


        // Final cleanup
        std::memset(_op_vector_rle, 0, State::MAX_VECTOR_SIZE * sizeof(uint32_t));
        op_filled_idx = 0;

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
        // Grab address of the output vector's selection mask
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
