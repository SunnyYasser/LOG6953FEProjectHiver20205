#ifndef SAMPLE_DB_OPERATOR_DEFINITION_HH
#define SAMPLE_DB_OPERATOR_DEFINITION_HH

#include <vector>
#include <string>
#include <memory>
#include "../../memory/include/context.hh"
#include "../../graph/include/datastore.hh"
#include "../../engine/include/pipeline.hh"
#include "operator_types.hh"

namespace SampleDB
{
    class Operator
    {
    public:
        Operator() = delete;
        Operator(const Operator &) = delete;
        Operator(const std::vector<std::string>);
        Operator(const std::vector<std::string>, std::shared_ptr<Operator>);

    public:
        const std::string get_uuid() const;
        const std::string get_operator_info () const;
        std::shared_ptr <Operator> get_next_operator ();
        virtual operator_type_t get_operator_type () const;
        std::vector <std::string> get_attributes () const;


    public:
        virtual void execute() = 0;
        virtual void debug() = 0;
        virtual void init(std::shared_ptr <ContextMemory>, std::shared_ptr <DataStore>) = 0;

    public:
        Vector _output_vector; // write enabled, need seperate copy
        Vector& _input_vector; // since read only, can live with a reference



    private:
        const std::string create_uuid();

    private:
        std::string _uuid;
        const std::vector<std::string> _columns;
        std::shared_ptr<Operator> _next_operator;
        operator_type_t _operator_type;
    };

}

#endif