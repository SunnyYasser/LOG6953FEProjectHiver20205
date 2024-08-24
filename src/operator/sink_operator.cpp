#include <iostream>
#include <exception>
#include <cassert>
#include "include/sink_operator.hh"
#include "include/operator_utils.hh"


namespace SampleDB
{
    int Sink::fixed_size_vector_cnt = 0;
    int Sink::total_size_if_materialized = 1;

    Sink::Sink(const std::vector<std::string>& columns) : Operator(columns, nullptr), _attribute (columns.front())
    {}

    operator_type_t Sink::get_operator_type () const
    {
        return OP_SINK;
    }

    void Sink::execute()
    {
        /*
        Sink operator prints the total size (rows * cols) if the vector from
        the previous operator is materialized
        */

        if (_input_vector.get_pos() != -1)
        {
            fixed_size_vector_cnt++;
        }

        total_size_if_materialized *= _input_vector.get_size();


        std::cout << "For Sink Operator "<< get_operator_name_as_string (get_operator_type (), get_uuid()) << " size stats are: \n";
        std::cout << "Total number of states with fixed value= " << fixed_size_vector_cnt << "\n";
        std::cout << "Total size output if materialized= " << total_size_if_materialized << "\n";
    }

    void Sink::debug()
    {
        log_operator_debug_msg (this);
        get_next_operator()->debug();
    }

    /*
    * We only need to create a read the data of given column (attribute)
    * Output vector is not required for sink operator
    */
    void Sink::init(std::shared_ptr <ContextMemory> context, std::shared_ptr <DataStore> datastore)
    {
        _input_vector = context->read_vector_for_attribute (_attribute);
    }

}
