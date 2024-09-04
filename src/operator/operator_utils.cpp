#include "include/operator_utils.hh"

#include <index_nested_loop_join_operator.hh>
#include <iostream>
#include <unordered_set>
#include "../utils/include/debug_enabled.hh"


namespace SampleDB {
    std::string get_operator_type_as_string(operator_type_t type) {
        switch (type) {
            case OP_GENERIC:
                return "GENERIC_OPERATOR";
            case OP_SCAN:
                return "SCAN_OPERATOR";
            case OP_INLJ:
                return "INLJ_OPERATOR";
            case OP_SINK:
                return "SINK_OPERATOR";
            case OP_SIZE:
                return "UNKNOWN_OPERATOR";
        }

        return "UNKNOWN_OPERATOR";
    }

    std::string get_operator_name_as_string(operator_type_t type, const std::string &uuid) {
        return get_operator_type_as_string(type) + " " + uuid;
    }

    std::string get_operator_name_as_string(const std::string &type, const std::string &uuid) {
        return type + " " + uuid;
    }

    void log_vector(const Vector &vector, const std::string &operator_info, const std::string &fn,
                    const std::string &table_name) {
        if (!is_debug_enabled())
            return;

        std::cout << "For operator : " << operator_info << "\n";
        std::cout << "In function : " << fn << "\n";
        std::cout << "For table : " << table_name << "\n";
        vector.print_debug_info();
        std::cout << "\n-------------------\n\n";
    }

    void log_vector(const Vector &ip_vector, const Vector &op_vector, const std::string &operator_info,
                    const std::string &fn, const std::string &table_name) {
        if (!is_debug_enabled())
            return;

        std::cout << "For Join operator : " << operator_info << "\n";
        std::cout << "In function : " << fn << "\n";
        std::cout << "For table : " << table_name << "\n";

        ip_vector.print_debug_info();
        op_vector.print_debug_info();

        std::cout << "\n-------------------\n\n";
    }


    void log_operator_debug_msg(const Operator *op) {
        if (!is_debug_enabled())
            return;

        std::cout << "OPERATOR DEBUG INFO:\n";
        std::cout << "Inside Operator : " << get_operator_name_as_string(op->get_operator_type(), op->get_uuid())
                  << "\n";

        std::cout << "For table : " << op->get_table_name() << "\n";

        std::cout << "\n";
        std::cout << "OPERATOR DEBUG INFO ENDS\n\n";
    }

    void remove_duplicates(std::vector<int32_t> vec, std::vector<int32_t> &_attribute_data) {
        std::unordered_set<int32_t> set{begin(vec), end(vec)};
        _attribute_data = {begin(set), end(set)};
    }


} // namespace SampleDB
