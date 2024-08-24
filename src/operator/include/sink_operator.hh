#ifndef SAMPLE_DB_SINK_OPERATOR_HH
#define SAMPLE_DB_SINK_OPERATOR_HH

#include "operator_definition.hh"
#include "operator_types.hh"

namespace SampleDB
{
    class Sink : public Operator
    {

    public:
        Sink() = delete;
        Sink(const Sink &) = delete;
        Sink(const std::vector<std::string>, std::shared_ptr<Operator>) = delete;
        Sink(const std::vector<std::string>&);


    public:
        void execute() override;
        void debug() override;
        void init(std::shared_ptr <ContextMemory>, std::shared_ptr <DataStore>) override;

    private:
        [[nodiscard]] operator_type_t get_operator_type () const override;
        std::string _attribute;
        static int fixed_size_vector_cnt;
        static int total_size_if_materialized;

    };
};

#endif