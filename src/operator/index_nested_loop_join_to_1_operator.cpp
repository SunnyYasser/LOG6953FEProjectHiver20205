#include "include/index_nested_loop_join_to_1_operator.hh"
#include <cmath>
#include "include/operator_utils.hh"


namespace VFEngine {
    IndexNestedLoopJointo1::IndexNestedLoopJointo1(const std::string &input_attribute,
                                                   const std::string &output_attribute, const bool &is_join_index_fwd,
                                                   const RelationType &relation_type,
                                                   const std::shared_ptr<Operator> &next_operator) :
        Operator(next_operator), _input_vector(nullptr), _output_vector(nullptr), _is_join_index_fwd(is_join_index_fwd),
        _relation_type(relation_type), _input_attribute(input_attribute), _output_attribute(output_attribute) {
#ifdef DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t IndexNestedLoopJointo1::get_operator_type() const { return OP_INLJ; }

    void IndexNestedLoopJointo1::execute() {
        const std::string fn_name = "IndexNestedLoopJointo1::execute";
        _exec_call_counter++;

        for (size_t idx = 0; idx < State::MAX_VECTOR_SIZE; idx++) {
            _output_vector->_values[idx] = ((*_adj_list)[_input_vector->_values[idx]]._size > 0) *
                                           _input_vector->_filter[idx] *
                                           (*_adj_list)[_input_vector->_values[idx]]._values[0];

            _output_vector->_filter[idx] =
                    _input_vector->_filter[idx] * ((*_adj_list)[_output_vector->_values[idx]]._size > 0);
        }

#ifdef DEBUG
        // log updated output vector
        _debug->log_vector(_input_vector, _output_vector, fn_name);
#endif

        // call next operator
        get_next_operator()->execute();
    }

    void IndexNestedLoopJointo1::init(const std::shared_ptr<ContextMemory> &context,
                                      const std::shared_ptr<DataStore> &datastore) {
#ifdef DEBUG
        _debug->log_operator_debug_msg();
#endif
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
