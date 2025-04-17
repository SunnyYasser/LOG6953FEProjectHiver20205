#ifndef VFENGINE_OPERATOR_DEBUG_HH
#define VFENGINE_OPERATOR_DEBUG_HH

#include <string>
#include "../../operator/include/operator_types.hh"

#include <fstream>

namespace VFEngine {
    class Vector;
    class Operator;

    class OperatorDebugUtility {
    public:
        explicit OperatorDebugUtility(const Operator *op);
        ~OperatorDebugUtility();
        void log_operator_debug_msg();
        void log_vector(const Vector *vector, const std::string &fn, const std::string &table_name = "R");
        void log_vector(const Vector *ip_vector, const Vector *op_vector, const std::string &fn,
                        const std::string &table_name = "R");
        static std::string get_operator_type_as_string(operator_type_t type);
        [[nodiscard]] std::string get_operator_name_as_string(operator_type_t type, const std::string &uuid);

    private:
        void create_logfile();
        const Operator *_operator;
        std::string _operator_name;
        std::ofstream _logfile;
    };
} // namespace VFEngine


#endif
