#include "include/index_nested_loop_join_operator.hh"
#include <algorithm>
#include <iostream>
#include "include/operator_utils.hh"


namespace SampleDB {
    IndexNestedLoopJoin::IndexNestedLoopJoin(const std::string &table, const std::string &input_attribute,
                                             const std::string &output_attribute, const bool reverse,
                                             const RelationType relation_type, const std::shared_ptr<Schema> &schema,
                                             const std::shared_ptr<Operator> &next_operator) :
        Operator(table, schema, next_operator), _reverse(reverse), _relation_type(relation_type),
        _states_sharing(false), _input_attribute(input_attribute),
        _output_attribute(output_attribute) {}

    operator_type_t IndexNestedLoopJoin::get_operator_type() const { return OP_INLJ; }

    void IndexNestedLoopJoin::execute_in_chunks_incremental() {
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_incremental()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());

        _schema->_schema_map[_input_attribute] = FLAT;
        _schema->_schema_map [_output_attribute] = UNFLAT;

        _input_vector.increment_pos();
        while (_input_vector.get_pos() < _input_vector.get_size()) {
            execute_internal (fn_name, operator_name);
            _input_vector.increment_pos();
        }
    }

    void IndexNestedLoopJoin::execute_in_chunks_non_incremental() {
        const std::string fn_name = "IndexNestedLoopJoin::execute_in_chunks_non_incremental()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());

        /*
         * no need to mark i/p FLAT here, already pos != -1
         */
        _schema->_schema_map [_output_attribute] = UNFLAT;

        execute_internal (fn_name, operator_name);
    }

    void IndexNestedLoopJoin::execute_internal (const std::string& fn_name, const std::string& operator_name) {
        const auto &_data_idx = _input_vector.get_pos();
        const auto &data = _input_vector.get_data_vector();
        const auto &newdata_values = _adj_list[data[_data_idx]];

        int32_t start_idx = 0;
        std::vector<int32_t> _chunked_data;

        while (start_idx < newdata_values.size()) {
            int32_t end_idx = std::min(start_idx + static_cast<int32_t>(newdata_values.size()) - 1,
                                       start_idx + static_cast<int32_t>(State::MAX_VECTOR_SIZE));

            auto start_itr = newdata_values.begin();
            std::advance(start_itr, start_idx);

            auto end_itr = newdata_values.begin();
            std::advance(end_itr, end_idx + 1);

            // TODO: copy happening here, must come up with a better way for slicing
            _chunked_data = std::vector<int32_t>(start_itr, end_itr);
            _output_vector = Vector(_chunked_data);

            log_vector(_input_vector, _output_vector, operator_name, fn_name);

            if (_states_sharing)
                _output_vector.set_state(_input_vector.get_state());

            _context_memory->update_column_data(_output_attribute, _output_vector);

            /*
             * For each element in source vector,
             */
            get_next_operator()->execute();

            start_idx = end_idx + 1;
        }
    }

    void IndexNestedLoopJoin::execute() {
        _input_vector = _context_memory->read_vector_for_column(_input_attribute, get_table_name());
        if (_input_vector.get_pos() == -1)
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
               (_relation_type == RelationType::MANY_TO_ONE and !_reverse) or
               (_relation_type == RelationType::ONE_TO_MANY and _reverse);
    }

    void IndexNestedLoopJoin::init(const std::shared_ptr<ContextMemory> &context,
                                   const std::shared_ptr<DataStore> &datastore) {
        _context_memory = context;
        _context_memory->allocate_memory_for_column(_output_attribute);
        _datastore = datastore;

        if (should_enable_state_sharing())
            _states_sharing = true;

        if (_reverse)
            _adj_list = _datastore->get_bwd_adj_list();
        else
            _adj_list = _datastore->get_fwd_adj_list();

        get_next_operator()->init(context, datastore);
    }
} // namespace SampleDB
