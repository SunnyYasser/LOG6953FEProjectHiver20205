#ifndef VFENGINE_SINK_OPERATOR_HH
#define VFENGINE_SINK_OPERATOR_HH

/*
 * Will always be one per logical plan
 * used to materialize the final output
 * of the query
 */

#include "operator_definition.hh"
#include "operator_types.hh"
#include "schema.hh"

namespace VFEngine {
    class Sink final : public Operator {
    public:
        Sink() = delete;

        Sink(const Sink &) = delete;

        explicit Sink(const std::unordered_map<std::string, SchemaType> &schema);

    public:
        void execute() override;
        void debug() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        static ulong get_total_row_size_if_materialized();
        ulong get_exec_call_counter() const;


    private:
        void update_total_row_size_if_materialized() const;
        void update_total_column_size_if_materialized();

        [[nodiscard]] operator_type_t get_operator_type() const override;
        std::unordered_map<std::string, SchemaType> _schema;
        std::vector<State *> _unique_states;
        static ulong total_row_size_if_materialized;
        ulong _exec_call_counter{};
    };
}; // namespace VFEngine

#endif
