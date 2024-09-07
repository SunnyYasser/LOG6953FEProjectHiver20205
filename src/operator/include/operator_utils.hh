#ifndef VFENGINE_OPERATOR_UTILS_HH
#define VFENGINE_OPERATOR_UTILS_HH

#include <string>
#include "../../memory/include/vector.hh"
#include "operator_definition.hh"
#include "operator_types.hh"

namespace VFEngine {
    void log_vector(const Vector &vector, const std::string &operator_info, const std::string &fn,
                    const std::string &table_name="R");

    void log_vector(const Vector &ip_vector, const Vector &op_vector, const std::string &operator_info,
                    const std::string &fn, const std::string &table_name="R");

    std::string get_operator_type_as_string(operator_type_t type);

    std::string get_operator_name_as_string(operator_type_t type, const std::string &uuid);

    std::string get_operator_name_as_string(const std::string &type, const std::string &uuid);

    void log_operator_debug_msg(const Operator *op);

    void remove_duplicates(std::vector<int32_t> vec, std::vector<int32_t> &_attribute_data);


} // namespace SampleDB

#endif
