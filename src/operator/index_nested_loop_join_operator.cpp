#include "include/index_nested_loop_join_operator.hh"
#include <algorithm>
#include <cmath>
#include <iostream>
#include "include/operator_utils.hh"


namespace VFEngine {
    IndexNestedLoopJoin::IndexNestedLoopJoin(const std::string &table, const std::string &input_attribute,
                                             const std::string &output_attribute, const bool &is_join_index_fwd,
                                             const RelationType &relation_type,
                                             const std::shared_ptr<Operator> &next_operator) :
        Operator(table, next_operator), _is_join_index_fwd(is_join_index_fwd), _relation_type(relation_type),
        _input_attribute(input_attribute), _output_attribute(output_attribute) {}

    operator_type_t IndexNestedLoopJoin::get_operator_type() const { return OP_INLJ; }

    void IndexNestedLoopJoin::execute_in_chunks_incremental() {
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_incremental()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());
        _input_vector._state->_pos++;
        while (_input_vector._state->_pos < _input_vector._state->_size) {
            execute_internal(fn_name, operator_name);
            _input_vector._state->_pos++;
        }
    }

    void IndexNestedLoopJoin::execute_in_chunks_non_incremental() {
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_non_incremental()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());
        execute_internal(fn_name, operator_name);
    }

    void IndexNestedLoopJoin::execute_internal(const std::string &fn_name, const std::string &operator_name) {
        const auto &_data_idx = _input_vector._state->_pos;
        const auto &data = _input_vector._vector;
        const auto &newdata_values = _adj_list[data[_data_idx]];
        const auto vector_size = newdata_values.size();
        constexpr auto chunk_size = State::MAX_VECTOR_SIZE;

        size_t number_chunks = std::ceil(static_cast<double>(vector_size) / chunk_size);
        size_t start = 0, end = 0;

        for (size_t chunk = 1; chunk <= number_chunks; ++chunk) {
            start = (chunk - 1) * chunk_size;

            // Branch less calculation of end index to handle leftover elements
            // If (max_id - start) >= chunk_size, then end = start + chunk_size
            // Else, end = max_id (remaining elements are less than chunk size)

            end = start + chunk_size * ((vector_size - start) >= chunk_size) +
                  (vector_size - start) * ((vector_size - start) < chunk_size);

            std::vector<uint64_t> _chunked_data(end - start + 1);

            auto start_itr = newdata_values.begin();
            std::advance(start_itr, start);

            auto end_itr = newdata_values.begin();
            std::advance(end_itr, end);

            // TODO: copy happening here, must come up with a better way for slicing
            _chunked_data = std::vector(start_itr, end_itr);
            _output_vector = Vector(_chunked_data);

            // update output vector for this operator
            _output_vector = Vector(_chunked_data);

            // log updated output vector
            log_vector(_input_vector, _output_vector, operator_name, fn_name);

            // update output vector in context
            _context_memory->update_column_data(_output_attribute, _output_vector);

            // call next operator
            get_next_operator()->execute();
        }
    }


    void IndexNestedLoopJoin::execute() {
        _input_vector = _context_memory->read_vector_for_column(_input_attribute, get_table_name());
        if (_input_vector._state->_pos == -1)
            execute_in_chunks_incremental();
        else
            execute_in_chunks_non_incremental();
    }


    void IndexNestedLoopJoin::debug() {
        log_operator_debug_msg(this);
        get_next_operator()->debug();
    }

    bool IndexNestedLoopJoin::should_enable_state_sharing() const {
        return _relation_type == RelationType::ONE_TO_ONE or
               (_relation_type == RelationType::MANY_TO_ONE and _is_join_index_fwd) or
               (_relation_type == RelationType::ONE_TO_MANY and !_is_join_index_fwd);
    }

    void IndexNestedLoopJoin::init(const std::shared_ptr<ContextMemory> &context,
                                   const std::shared_ptr<DataStore> &datastore) {
        _context_memory = context;
        _context_memory->allocate_memory_for_column(_output_attribute);
        _datastore = datastore;

        if (should_enable_state_sharing()) {
            auto &ip_vector = _context_memory->read_vector_for_column(_input_attribute, get_table_name());
            auto &op_vector = _context_memory->read_vector_for_column(_output_attribute, get_table_name());
            op_vector._state = ip_vector._state;
        }

        if (_is_join_index_fwd)
            _adj_list = _datastore->get_fwd_adj_list();
        else
            _adj_list = _datastore->get_bwd_adj_list();

        get_next_operator()->init(context, datastore);
    }
} // namespace VFEngine
