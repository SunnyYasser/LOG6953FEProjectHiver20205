#ifndef SAMPLE_DB_SINK_OPERATOR_HH
#define SAMPLE_DB_SINK_OPERATOR_HH

/*
 * Will always be one per logical plan
 * used to materialize the final output
 * of the query
 */

#include "operator_definition.hh"
#include "operator_types.hh"

#include <unordered_set>

namespace SampleDB {
    class Sink : public Operator {
    public:
        Sink() = delete;

        Sink(const Sink &) = delete;

        Sink (const std::string&, const std::vector <std::string>&);
    public:
        void execute() override;

        void debug() override;

        void init(std::shared_ptr<ContextMemory>, std::shared_ptr<DataStore>) override;

    private:
        void update_total_row_size_if_materialized(const std::string &, const std::string &, const Vector &);
        void update_total_column_size_if_materialized(const std::string &, bool);
        void update_total_column_size_if_materialized(const std::string &);



    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;

        static int fixed_size_vector_cnt;
        static int total_row_size_if_materialized;
        static int total_column_size_if_materialized;
        const std::string _input_attribute;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _datastore;

    };
};

#endif
