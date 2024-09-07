#include "include/scan_operator.hh"
#include <cassert>
#include <unordered_set>
#include "include/operator_utils.hh"

namespace VFEngine {
    Scan::Scan(const std::string &table_name, const std::string &scan_attribute,
               const std::shared_ptr<Operator> &next_operator) :
        Operator(table_name, next_operator), _attribute(scan_attribute), _attribute_data({}) {}

    operator_type_t Scan::get_operator_type() const { return OP_SCAN; }

    /*
    This routine ingests in a data vector, and processes it in chunks of
    size CHUNK_SIZE (1024)
    */

    void Scan::execute() {
        const std::string fn_name = "Scan::execute()";
        const std::string operator_name = get_operator_name_as_string(get_operator_type(), get_uuid());

        int32_t start_idx = 0;
        while (start_idx < _attribute_data.size()) {
            int32_t end_idx = std::min(start_idx + static_cast<int32_t>(_attribute_data.size()) - 1,
                                       start_idx + static_cast<int32_t>(State::MAX_VECTOR_SIZE));
            auto _chunked_data =
                    std::vector<int32_t>(begin(_attribute_data) + start_idx, begin(_attribute_data) + end_idx + 1);

            // update output vector for this operator
            _output_vector = Vector(_chunked_data);

            // log updated output vector
            log_vector(_output_vector, operator_name, fn_name, get_table_name());

            // update output vector in context
            _context_memory->update_column_data(_attribute, _output_vector);

            // increment next chunk starting idx
            start_idx = end_idx + 1;

            // call next operator
            get_next_operator()->execute();
        }
    }

    void Scan::debug() {
        log_operator_debug_msg(this);
        get_next_operator()->debug();
    }

    /*
     * We only need to create a copy of the data of given column (attribute)
     * Input vector is not required for scan operator
     */
    void Scan::init(const std::shared_ptr<ContextMemory> &context, const std::shared_ptr<DataStore> &datastore) {
        _context_memory = context;
        _context_memory->allocate_memory_for_column(_attribute, get_table_name());

        const auto &adj_list = datastore->get_fwd_adj_list();
        for (const auto &[k, _]: adj_list) {
            _attribute_data.push_back(k);
        }

        get_next_operator()->init(context, datastore);
    }
} // namespace VFEngine
