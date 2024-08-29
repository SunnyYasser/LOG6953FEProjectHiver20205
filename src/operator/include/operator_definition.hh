#ifndef SAMPLE_DB_OPERATOR_DEFINITION_HH
#define SAMPLE_DB_OPERATOR_DEFINITION_HH

#include <vector>
#include <string>
#include <memory>
#include "../../memory/include/context.hh"
#include "../../graph/include/datastore.hh"
#include "operator_types.hh"

namespace SampleDB {
    class Operator {
    public:
        Operator();

        Operator(const Operator &) = delete;

        Operator(const std::string &, const std::vector<std::string> &, std::shared_ptr<Operator>);

        virtual ~Operator() = default;

    public:
        [[nodiscard]] std::string get_uuid() const;

        [[nodiscard]] std::string get_table_name() const;

        [[nodiscard]] std::string get_operator_info() const;

        std::shared_ptr<Operator> get_next_operator();

        [[nodiscard]] virtual operator_type_t get_operator_type() const;

        [[nodiscard]] std::vector<std::string> get_attributes() const;

    public:
        virtual void execute() = 0;

        virtual void debug() = 0;

        virtual void init(std::shared_ptr<ContextMemory>, std::shared_ptr<DataStore>) = 0;

    public:
        Vector _output_vector;
        Vector& _input_vector;

    private:
        static std::string create_uuid();

    private:
        std::string _uuid;
        std::string _table_name;
        const std::vector<std::string> _columns;
        std::shared_ptr<Operator> _next_operator;
        operator_type_t _operator_type;
    };
}

#endif
