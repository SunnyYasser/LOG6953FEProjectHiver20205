#ifndef SAMPLE_DB_OPERATOR_DEFINITION_HH
#define SAMPLE_DB_OPERATOR_DEFINITION_HH

#include <memory>
#include <string>
#include "../../graph/include/datastore.hh"
#include "../../memory/include/context.hh"
#include "operator_types.hh"
#include "schema.hh"

namespace SampleDB {
    class Operator {
    public:
        Operator() = delete;
        Operator(const Operator &) = delete;

        explicit Operator(const std::shared_ptr<Schema> &);

        Operator(const std::string &, const std::shared_ptr<Schema> &, const std::shared_ptr<Operator> &);

        virtual ~Operator() = default;

    public:
        [[nodiscard]] std::string get_uuid() const;

        [[nodiscard]] std::string get_table_name() const;

        [[nodiscard]] std::string get_operator_info() const;

        std::shared_ptr<Operator> get_next_operator();

        [[nodiscard]] virtual operator_type_t get_operator_type() const;

    public:
        virtual void execute() = 0;

        virtual void debug() = 0;

        virtual void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) = 0;

        std::shared_ptr<Schema> _schema;

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
} // namespace SampleDB

#endif
