#include "include/scan_failure_prop_operator.hh"
#include <cassert>
#include <cmath>
#include <cstring>
#include <numeric>
#include "include/operator_utils.hh"

namespace VFEngine {
    ScanFailureProp::ScanFailureProp(const std::string &scan_attribute, const std::vector<uint64_t>& src_nodes, const std::shared_ptr<Operator> &next_operator):
        Operator(next_operator), _output_vector(nullptr), _output_selection_mask(nullptr),
        _attribute(scan_attribute), _src_nodes(src_nodes) {
    }

    operator_type_t ScanFailureProp::get_operator_type() const { return OP_SCAN; }

    void ScanFailureProp::execute() {
        _exec_call_counter++;
        constexpr size_t chunk_size = State::MAX_VECTOR_SIZE;
        const size_t number_chunks = std::ceil(static_cast<double>(_src_nodes.size()) / chunk_size);
        size_t start_idx = 0;

        for (size_t chunk = 1; chunk <= number_chunks; ++chunk) {
            start_idx = (chunk - 1) * chunk_size;

            // Calculate the number of elements in the current chunk
            const size_t remaining_elements = _src_nodes.size() - start_idx;
            const size_t _curr_chunk_size = std::min(remaining_elements, chunk_size);

            // Copy values from src_nodes to output vector for the current chunk
            for (size_t i = 0; i < _curr_chunk_size; ++i) {
                _output_vector->_values[i] = _src_nodes[start_idx + i];
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
    void ScanFailureProp::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        context->allocate_memory_for_column(_attribute);
        _output_vector = context->read_vector_for_column(_attribute);
        _output_vector->allocate_rle();
        _output_vector->allocate_selection_bitmask();
        _output_selection_mask = &(_output_vector->_state->_selection_mask);
        get_next_operator()->init(context, datastore);
    }

    unsigned long ScanFailureProp::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
