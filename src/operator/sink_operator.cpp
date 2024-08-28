#include <iostream>
#include <vector>
#include "include/operator_utils.hh"
#include "include/sink_operator.hh"


namespace SampleDB {
    int Sink::fixed_size_vector_cnt = 0;
    int Sink::total_row_size_if_materialized = 1;
    int Sink::total_column_size_if_materialized = 0;


    Sink::Sink(const std::unordered_map<std::string, std::vector<std::string> > &table_to_columns_map) : Operator(),
        _table_to_columns_map(table_to_columns_map) {
    }

    operator_type_t Sink::get_operator_type() const {
        return OP_SINK;
    }

    void Sink::execute() {
        /*
         *Iterate over all the vectors present in the context
         *update size as required
         */

        for (const auto &[table, columns]: _table_to_columns_map) {
            /*
            Sink operator prints the total size (rows * cols) if the vector from
            the previous operator is materialized
            */

            total_column_size_if_materialized += _datastore->get_table_columns_size(table);


            for (const auto &column: columns) {
                bool result = false;
                const auto &_input_vector = _context_memory->read_vector_for_column(table, column, result);

                if (!result) {
                    continue;
                }

                if (_input_vector.get_pos() != -1) {
                    fixed_size_vector_cnt++;
                }
                else {
                    total_column_size_if_materialized -=1 ; // -1 implies a join, column will be counted twice
                }

                update_size(table, column, _input_vector);

                std::cout << "For Sink Operator " << get_operator_name_as_string(get_operator_type(), get_uuid()) <<
                        " size stats are: \n";
                std::cout << "Total number of states with fixed value= " << fixed_size_vector_cnt << "\n";
                std::cout << "Total size output if materialized= " << total_row_size_if_materialized << " x " <<
                        total_column_size_if_materialized << "\n";
            }
        }
    }

    void Sink::update_size(const std::string &table, const std::string &column, const Vector &vector) {
        if (vector.get_pos() == -1) {
            total_row_size_if_materialized *= vector.get_size(); //cartesian product
        } else {
            // TODO not sure about this
            //total_row_size_if_materialized += vector.get_size(); //simple append at the bottom of result table
        }
    }

    void Sink::debug() {
        log_operator_debug_msg(this);
    }

    /*
    * We only need to create a read the data of given column (attribute)
    * Output vector is not required for sink operator
    */
    void Sink::init(std::shared_ptr<ContextMemory> context, std::shared_ptr<DataStore> datastore) {
        _context_memory = context;
        _datastore = datastore;
    }
}
