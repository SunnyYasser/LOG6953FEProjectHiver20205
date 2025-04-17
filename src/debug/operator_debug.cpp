#include "include/operator_debug.hh"
#include <filesystem>
#include <fstream>
#include "../memory/include/vector.hh"
#include "../operator/include/operator_definition.hh"
#include "../utils/include/debug_enabled.hh"
#include "../utils/include/testpaths.hh"

namespace VFEngine {
    OperatorDebugUtility::OperatorDebugUtility(const Operator *op) : _operator(op) {
        _operator_name = get_operator_name_as_string(_operator->get_operator_type(), _operator->get_uuid());
        create_logfile();
    }

    OperatorDebugUtility::~OperatorDebugUtility() {
        if (_logfile and _logfile.is_open()) {
            _logfile.close();
        }
    }

    void OperatorDebugUtility::create_logfile() {
        if (!is_operator_debug_enabled())
            return;

        const auto op_folder = get_operator_debug_path();
        std::string folder;
        if (!op_folder) {
            folder = "operator_debug_logs";
        } else {
            folder = op_folder;
        }

        std::filesystem::create_directories(folder);
        folder += "/" + _operator_name;
        _logfile.open(folder);
    }

    std::string OperatorDebugUtility::get_operator_type_as_string(operator_type_t type) {
        switch (type) {
            case OP_GENERIC:
                return "GENERIC_OPERATOR";
            case OP_SCAN:
                return "SCAN_OPERATOR";
            case OP_INLJ:
                return "INLJ_OPERATOR";
            case OP_SINK:
                return "SINK_OPERATOR";
            case OP_INLJ_PACKED:
                return "INLJ_OPERATOR_PACKED";
            case OP_INLJ_NTO1:
                return "INLJ_OPERATOR_NTO1";
            case OP_SINK_PACKED_VECTORIZED:
                return "SINK_OPERATOR_PACKED_VECTORIZED";
            case OP_SINK_PACKED_MIN:
                return "SINK_OPERATOR_PACKED_MIN";
            case OP_SINK_PACKED_HARDCODED_LINEAR:
                return "SINK_OPERATOR_PACKED_HARDCODED_LINEAR";
            case OP_CASCADE_SELECTION:
                return "CASCADE_SELECTION_OPERATOR";
            case OP_SIZE:
                return "UNKNOWN_OPERATOR";
        }

        return "UNKNOWN_OPERATOR";
    }

    std::string OperatorDebugUtility::get_operator_name_as_string(operator_type_t type, const std::string &uuid) {
        return get_operator_type_as_string(type) + "_" + uuid;
    }

    void OperatorDebugUtility::log_operator_debug_msg() {
        if (!is_operator_debug_enabled() or !_logfile.is_open())
            return;

        _logfile << "OPERATOR DEBUG INFO:\n";
        _logfile << "Inside Operator : " << _operator_name << "\n";
    }

    void OperatorDebugUtility::log_vector(const Vector *vector, const std::string &fn, const std::string &table_name) {
        if (!is_operator_debug_enabled() or !_logfile.is_open())
            return;

        _logfile << "In function : " << fn << "\n";
        _logfile << "For table : " << table_name << "\n";
        vector->print_debug_info(_logfile);
        _logfile << "\n-------------------\n\n";
    }
    void OperatorDebugUtility::log_vector(const Vector *ip_vector, const Vector *op_vector, const std::string &fn,
                                          const std::string &table_name) {
        if (!is_operator_debug_enabled() or !_logfile.is_open())
            return;

        _logfile << "In function : " << fn << "\n";
        _logfile << "For table : " << table_name << "\n";
        ip_vector->print_debug_info(_logfile);
        op_vector->print_debug_info(_logfile);
        _logfile << "\n-------------------\n\n";
    }

} // namespace VFEngine
