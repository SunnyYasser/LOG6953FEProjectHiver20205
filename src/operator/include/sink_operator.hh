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
#ifdef MY_DEBUG
#include "../../debug/include/operator_debug.hh"
#endif


namespace VFEngine {
    class Sink final : public Operator {
    public:
        Sink() = delete;

        Sink(const Sink &) = delete;

        explicit Sink(const std::vector<std::pair<std::string, SchemaType>> &schema);

        void execute() override;
        void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) override;
        static ulong get_total_row_size_if_materialized();
        [[nodiscard]] unsigned long get_exec_call_counter() const override;


    private:
        void update_total_row_size_if_materialized() const;
        void update_total_column_size_if_materialized();

        [[nodiscard]] operator_type_t get_operator_type() const override;
        std::vector<std::pair<std::string, SchemaType>> _schema;
        std::vector<State *> _unique_states;
        static unsigned long total_row_size_if_materialized;
        unsigned long _exec_call_counter{};
#ifdef MY_DEBUG
        std::unique_ptr<OperatorDebugUtility> _debug;
#endif
    };
}; // namespace VFEngine

#endif
