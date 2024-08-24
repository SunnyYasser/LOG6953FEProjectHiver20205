#include "include/operator_utils.hh"
#include "../utils/include/debug_enabled.h"
#include <iostream>


namespace SampleDB
{
    const std::string get_operator_type_as_string (operator_type_t type)
    {
        switch (type)
        {
            case OP_GENERIC: return "GENERIC_OPERATOR";
            case OP_SCAN: return "SCAN_OPERATOR";
            case OP_INLJ: return "INLJ_OPERATOR";
            case OP_SINK: return "SINK_OPERATOR";
            case OP_SIZE: return "UNKNOWN_OPERATOR";
        }

        return "UNKNOWN_OPERATOR";
    }

    const std::string get_operator_name_as_string (operator_type_t type, const std::string& uuid)
    {
        return get_operator_type_as_string (type) + " " + uuid;
    }

    const std::string get_operator_name_as_string (const std::string& type, const std::string& uuid)
    {
        return type + " " + uuid;
    }

    void log_vector (const Vector& vector, const std::string& operator_info, const std::string& fn)
    {
        if (!is_debug_enabled()) return;

        std::cout << "For operator : " <<  operator_info << "\n";
        std::cout << "In function : " << fn << "\n";
        vector.print_debug_info();
        std::cout << "\n-------------------\n";
    }

    void log_operator_debug_msg (const Operator * op)
    {
        if (!is_debug_enabled()) return;

        std::cout << "OPERATOR DEBUG INFO:\n";
        std::cout << "Inside Operator : " << get_operator_name_as_string (op->get_operator_type (), op->get_uuid()) << "\n";
        std::cout << "Attribute name(s): " << "\n";
        auto&& attributes = op->get_attributes ();
        for (auto& attribute : attributes)
        {
            std::cout << attribute << " ";
        }
        std::cout << "\n";
        std::cout << "OPERATOR DEBUG INFO ENDS\n";
    }
}


