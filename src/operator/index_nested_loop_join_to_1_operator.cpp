#include "include/index_nested_loop_join_to_1_operator.hh"
#include <cmath>
#include "include/operator_utils.hh"


namespace VFEngine {
    IndexNestedLoopJointo1::IndexNestedLoopJointo1(const std::string &input_attribute,
                                                   const std::string &output_attribute, const bool &is_join_index_fwd,
                                                   const RelationType &relation_type,
                                                   const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _input_vector(nullptr), _output_vector(nullptr), _is_join_index_fwd(is_join_index_fwd),
        _relation_type(relation_type), _input_attribute(input_attribute), _output_attribute(output_attribute) {}

    operator_type_t IndexNestedLoopJointo1::get_operator_type() const { return OP_INLJ; }

    void IndexNestedLoopJointo1::execute() {
        const std::string fn_name = "IndexNestedLoopJointo1::execute";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());
        _exec_call_counter++;

        for (size_t idx = 0; idx < State::MAX_VECTOR_SIZE; idx++) {
            _output_vector->_values[idx] = ((*_adj_list)[_input_vector->_values[idx]]._size > 0) *
                                           (*_adj_list)[_input_vector->_values[idx]]._values[0];
        }

        // log updated output vector
        log_vector(_input_vector, _output_vector, operator_name, fn_name);

        // call next operator
        get_next_operator()->execute();
    }


    void IndexNestedLoopJointo1::debug() {
        log_operator_debug_msg(this);
        get_next_operator()->debug();
    }

    void IndexNestedLoopJointo1::init(const std::shared_ptr<ContextMemory> &context,
                                      const std::shared_ptr<DataStore> &datastore) {

        // enable state sharing
        context->allocate_memory_for_column(_output_attribute, _input_attribute, true);

        _input_vector = context->read_vector_for_column(_input_attribute);
        _output_vector = context->read_vector_for_column(_output_attribute);

        if (_is_join_index_fwd)
            _adj_list = &(datastore->get_fwd_adj_lists());
        else
            _adj_list = &(datastore->get_bwd_adj_lists());

        get_next_operator()->init(context, datastore);
    }

    ulong IndexNestedLoopJointo1::get_exec_call_counter() const { return _exec_call_counter; }

} // namespace VFEngine
