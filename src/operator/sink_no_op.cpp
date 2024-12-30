#include "include/sink_no_op.hh"
#include <iostream>
#include <vector>
#include "include/operator_utils.hh"


namespace VFEngine {
    SinkNoOp::SinkNoOp() : Operator() {
#ifdef MY_DEBUG
        _debug = std::make_unique<OperatorDebugUtility>(this);
#endif
    }

    operator_type_t SinkNoOp::get_operator_type() const { return OP_SINK_NO_OP; }

    void SinkNoOp::execute() { _exec_call_counter++; }

    /*
     * We only need to create a read the data of given column (attribute)
     * Output vector is not required for sink operator
     */
    void SinkNoOp::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {}

    ulong SinkNoOp::get_exec_call_counter() const { return _exec_call_counter; }

    ulong SinkNoOp::get_total_row_size_if_materialized() { return 0; }

} // namespace VFEngine
