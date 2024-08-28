#ifndef SAMPLE_DB_SINK_OPERATOR_HH
#define SAMPLE_DB_SINK_OPERATOR_HH

/*
 * Will always be one per logical plan
 * used to materialize the final output
 * of the query
 */

#include "operator_definition.hh"
#include "operator_types.hh"

namespace SampleDB {
    class Sink : public Operator {
    public:
        Sink() = delete;

        Sink(const Sink &) = delete;

        Sink(const std::vector<std::string> &, std::shared_ptr<Operator>) = delete;

        Sink(const std::unordered_map<std::string, std::vector<std::string> > &);

    public:
        void execute() override;

        void debug() override;

        void init(std::shared_ptr<ContextMemory>, std::shared_ptr<DataStore>) override;

    private:
        void update_size(const std::string &, const std::string &, const Vector &);

    private:
        [[nodiscard]] operator_type_t get_operator_type() const override;

        std::vector<std::pair<std::string, std::string> > _table_column_operation_pairs;
        static int fixed_size_vector_cnt;
        static int total_row_size_if_materialized;
        static int total_column_size_if_materialized;

        std::unordered_map<std::string, std::vector<std::string> > _table_to_columns_map;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _datastore;
    };
};

#endif
