#ifndef SAMPLE_DB_SINK_OPERATOR_HH
#define SAMPLE_DB_SINK_OPERATOR_HH

/*
 * Will always be one per logical plan
 * used to materialize the final output
 * of the query
 */

#include "operator_definition.hh"
#include "operator_types.hh"
#include "schema.hh"
#include "../../memory/include/state_hash.hh"

#include <unordered_set>

namespace SampleDB {
    class Sink : public Operator {
    public:
        Sink() = delete;

        Sink(const Sink &) = delete;

        explicit Sink (const std::shared_ptr<Schema>& schema);
    public:
        void execute() override;

        void debug() override;

        void init(const std::shared_ptr<ContextMemory>&, const std::shared_ptr<DataStore>&) override;

    private:
        void update_total_row_size_if_materialized() const;
        void update_total_column_size_if_materialized();

        [[nodiscard]] operator_type_t get_operator_type() const override;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _datastore;
        std::unordered_set<std::shared_ptr<State>, StateSharedPtrHash, StateSharedPtrEqual> _unique_states;
        static long total_row_size_if_materialized;
        static long total_column_size_if_materialized;

    };
};

#endif
