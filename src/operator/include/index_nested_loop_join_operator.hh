//
// Created by Sunny on 16-06-2024.
//

#ifndef SAMPLE_DB_INDEX_NESTED_LOOP_JOIN_OPERATOR_HH
#define SAMPLE_DB_INDEX_NESTED_LOOP_JOIN_OPERATOR_HH

#include "../../memory/include/vector.hh"
#include "operator_definition.hh"
#include "operator_types.hh"

namespace SampleDB {
    class IndexNestedLoopJoin : public Operator {
    public:
        IndexNestedLoopJoin() = delete;

        IndexNestedLoopJoin(const IndexNestedLoopJoin &) = delete;

        IndexNestedLoopJoin(const std::vector<std::string> &) = delete;

        IndexNestedLoopJoin(const std::string &, const std::string &,
                            const std::vector<std::string> &,
                            std::shared_ptr<Operator>);

        void execute() override;

        void debug() override;

        void init(std::shared_ptr<ContextMemory>, std::shared_ptr<DataStore>) override;

    private:
        void execute_in_chunks();

        [[nodiscard]] operator_type_t get_operator_type() const override;

    private:
        Vector create_new_state(const Vector &);

    private:
        const std::string _input_attribute, _output_attribute, _right_table, _left_table;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _datastore;
    };
};

#endif
