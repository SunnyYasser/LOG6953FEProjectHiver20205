#include "include/sink_operator.hh"
#include <iostream>
#include <vector>
#include "include/operator_utils.hh"


namespace SampleDB {
    int Sink::fixed_size_vector_cnt = 0;
    int Sink::total_row_size_if_materialized = 1;
    int Sink::total_column_size_if_materialized = 0;

    Sink::Sink(const std::string &table_name, const std::vector<std::string> &column) :
        Operator(table_name, column, nullptr) {}

    operator_type_t Sink::get_operator_type() const { return OP_SINK; }

    void Sink::execute() {
        _input_vector = _context_memory->read_vector_for_column(get_table_name(), _input_attribute);

        if (_input_vector.get_pos() != -1) {
            fixed_size_vector_cnt++;
        } else {
            update_total_row_size_if_materialized(get_table_name(), _input_attribute, _input_vector);
        }


        std::cout << "For Sink Operator " << get_operator_name_as_string(get_operator_type(), get_uuid())
                  << " size stats are: \n";
        std::cout << "Total number of states with fixed value= " << fixed_size_vector_cnt << "\n";
        std::cout << "Total size output if materialized= " << total_row_size_if_materialized << "\n";
    }


    void Sink::update_total_row_size_if_materialized(const std::string &table, const std::string &column,
                                                     const Vector &vector) {
        if (vector.get_pos() == -1) {
            auto new_rows = 1 * vector.get_size();
            total_row_size_if_materialized += new_rows; // cartesian product
        } else {
            // TODO not sure about this
            // total_row_size_if_materialized += vector.get_size(); //simple append at the bottom of result table
        }
    }

    void Sink::update_total_column_size_if_materialized(const std::string &table) {}

    void Sink::update_total_column_size_if_materialized(const std::string &table, bool join) {}

    void Sink::debug() { log_operator_debug_msg(this); }

    /*
     * We only need to create a read the data of given column (attribute)
     * Output vector is not required for sink operator
     */
    void Sink::init(std::shared_ptr<ContextMemory> context, std::shared_ptr<DataStore> datastore) {
        _context_memory = context;
        _datastore = datastore;
    }
} // namespace SampleDB
