#ifndef SAMPLE_DB_PIPELINE_HH
#define SAMPLE_DB_PIPELINE_HH

#include <memory>
#include <vector>
#include <string>
#include "../../memory/include/context.hh"
#include "../../operator/include/operator_definition.hh"
#include "../../graph/include/datastore.hh"

namespace SampleDB
{
    class Operator;

    class Pipeline
    {
    public:
        Pipeline() = delete;
        explicit Pipeline(const std::vector<std::string> &, const std::unordered_map<std::string, std::vector<std::string>>&);

    public:
        void debug() const;
        void init() const;
        void execute() const;
        void clear() const;
        std::shared_ptr<SampleDB::ContextMemory> get_context_memory();
        std::shared_ptr<SampleDB::DataStore> get_datastore();
        void set_first_operator(const std::shared_ptr<SampleDB::Operator> &);

    private:
        const std::vector<std::string> _tables;
        std::shared_ptr<SampleDB::Operator> _first_operator;
        std::shared_ptr<SampleDB::ContextMemory> _context_memory;
        std::shared_ptr<SampleDB::DataStore> _datastore;
    };

}


#endif