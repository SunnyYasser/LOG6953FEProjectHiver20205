#ifndef VFENGINE_OPERATOR_DEFINITION_HH
#define VFENGINE_OPERATOR_DEFINITION_HH

#include <memory>
#include <string>
#include "../../graph/include/datastore.hh"
#include "../../memory/include/context.hh"
#include "operator_types.hh"
#include "schema.hh"

namespace VFEngine {
    class Operator {
    public:
        Operator();
        Operator(const Operator &) = delete;
        Operator(const std::string &table_name, const std::shared_ptr<Operator> &next_operator);
        virtual ~Operator() = default;

        [[nodiscard]] std::string get_uuid() const;

        [[nodiscard]] std::string get_table_name() const;

        [[nodiscard]] std::string get_operator_info() const;

        std::shared_ptr<Operator> get_next_operator();

        [[nodiscard]] virtual operator_type_t get_operator_type() const;

        virtual void execute() = 0;

        virtual void debug() = 0;

        virtual void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) = 0;

    public:
        Vector _output_vector;
        Vector _input_vector;

    private:
        static std::string create_uuid();

    private:
        std::string _uuid;
        std::string _table_name;
        std::shared_ptr<Operator> _next_operator;
        operator_type_t _operator_type;
        const std::shared_ptr<ContextMemory> _context_memory;
        const std::shared_ptr<DataStore> _data_store;
    };
} // namespace VFEngine

#endif
