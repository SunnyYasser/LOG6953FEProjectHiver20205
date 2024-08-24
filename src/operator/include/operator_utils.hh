#ifndef SAMPLE_DB_OPERATOR_UTILS_HH
#define SAMPLE_DB_OPERATOR_UTILS_HH

#include "operator_types.hh"
#include "operator_definition.hh"
#include "../../memory/include/vector.hh"
#include "../../memory/include/state.hh"
#include <string>

namespace SampleDB
{
    void log_vector (const Vector&, const std::string&, const std::string&);
    const std::string get_operator_type_as_string (operator_type_t);
    const std::string get_operator_name_as_string (operator_type_t, const std::string&);
    const std::string get_operator_name_as_string (const std::string&, const std::string&);
    void log_operator_debug_msg (const Operator *);
}

#endif
