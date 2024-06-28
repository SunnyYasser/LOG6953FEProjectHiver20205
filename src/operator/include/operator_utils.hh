#ifndef SAMPLE_DB_OPERATOR_UTILS_HH
#define SAMPLE_DB_OPERATOR_UTILS_HH

#include "operator_types.hh"
#include "operator_definition.hh"
#include "../../memory/include/vector.hh"
#include "../../memory/include/state.hh"
#include <string>

namespace SampleDB
{
    const std::string get_operator_type_as_string (operator_type_t type);
    const std::string get_operator_name_as_string (operator_type_t type, const std::string& uuid);
    const std::string get_operator_name_as_string (const std::string& type, const std::string& uuid);
    void log_vector (const Vector& vector, const std::string& operator_info, const std::string& fn);
    void log_operator_debug_msg (const Operator *);
}

#endif
