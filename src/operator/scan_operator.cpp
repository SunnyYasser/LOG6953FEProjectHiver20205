#include "include/scan_operator.hh"
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstring>
#include <numeric>
#include "include/operator_utils.hh"

namespace VFEngine {
    Scan::Scan(const std::string &scan_attribute, const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _output_vector(nullptr), _max_id_value(0), _attribute(scan_attribute),
        _output_selection_mask(nullptr) {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t Scan::get_operator_type() const { return OP_SCAN; }

    /*
    This routine ingests in a data vector, and processes it in chunks of
    size CHUNK_SIZE (1024)
    */

    void Scan::execute() {
        _exec_call_counter++;
        const std::string fn_name = "Scan::execute()";
        constexpr auto chunk_size = State::MAX_VECTOR_SIZE;
        const size_t number_chunks = std::ceil(static_cast<double>(_max_id_value + 1) / chunk_size);
        size_t start = 0, end = 0;

        for (size_t chunk = 1; chunk <= number_chunks; ++chunk) {
            start = (chunk - 1) * chunk_size;

            // Branch less calculation of end index to handle leftover elements
            // If (max_id - start) >= chunk_size, then end = start + chunk_size
            // Else, end = max_id (remaining elements are less than chunk size)

            end = start + chunk_size * ((_max_id_value - start) >= chunk_size) +
                  (_max_id_value - start + 1) * ((_max_id_value - start) < chunk_size);

            const size_t _curr_chunk_size = end - start;

            for (size_t i = 0; i < _curr_chunk_size; ++i) {
                _output_vector->_values[i] = start + i;
            }

#ifdef MY_DEBUG
            assert(_output_selection_mask != nullptr);
            assert(_curr_chunk_size <= State::MAX_VECTOR_SIZE);
            assert(_output_vector->_state->_rle != nullptr);
#endif

            _output_vector->_state->_state_info._size = static_cast<int32_t>(_curr_chunk_size);
            _output_vector->_state->_state_info._pos = -1;

            // only needed for INLJ Packed
            // Set start and end position in the selection mask
            // Mark all bits as valid
            SET_START_POS(**_output_selection_mask, 0);
            SET_END_POS(**_output_selection_mask, _curr_chunk_size - 1);
            SET_BITS_TILL_IDX(**_output_selection_mask, _curr_chunk_size - 1);
            // Initialize output RLE

#ifdef VECTOR_STATE_ARENA_ALLOCATOR
            std::memset(_output_vector->_state->_rle, 0, (State::MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
#else
            std::memset(_output_vector->_state->_rle.get(), 0, (State::MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
#endif

#ifdef MY_DEBUG
            // log updated output vector
            _debug->log_vector(_output_vector, fn_name);
#endif
            // call next operator
            get_next_operator()->execute();

            // these two only needed for backward compatibility with Base INLJ
            _output_vector->_state->_state_info._size = 0;
            _output_vector->_state->_state_info._pos = -1;

            // only needed for Packed INLJ
            SET_ALL_BITS(**_output_selection_mask);
            SET_START_POS(**_output_selection_mask, 0);
            SET_END_POS(**_output_selection_mask, State::MAX_VECTOR_SIZE - 1);
            // Reset output RLE
#ifdef VECTOR_STATE_ARENA_ALLOCATOR
            std::memset(_output_vector->_state->_rle, 0, (State::MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
#else
            std::memset(_output_vector->_state->_rle.get(), 0, (State::MAX_VECTOR_SIZE + 1) * sizeof(uint32_t));
#endif
        }
    }

    /*
     * We only need to create a copy of the data of given column (attribute)
     * Input vector is not required for scan operator
     */
    void Scan::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
#ifdef MY_DEBUG
        _debug->log_operator_debug_msg();
#endif
        context->allocate_memory_for_column(_attribute);
        _output_vector = context->read_vector_for_column(_attribute);
        _output_vector->allocate_rle();
        _output_vector->allocate_selection_bitmask();
        _output_selection_mask = &(_output_vector->_state->_selection_mask);
        _max_id_value = datastore->get_max_id_value();
        get_next_operator()->init(context, datastore);
    }

    unsigned long Scan::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
