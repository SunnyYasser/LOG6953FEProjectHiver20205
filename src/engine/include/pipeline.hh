#ifndef VFENGINE_PIPELINE_HH
#define VFENGINE_PIPELINE_HH

#include <memory>
#include <string>
#include <vector>
#include "../../graph/include/datastore.hh"
#include "../../memory/include/context.hh"
#include "../../operator/include/operator_definition.hh"

namespace VFEngine {
    class Operator;

    class Pipeline {
    public:
        Pipeline() = delete;
        Pipeline(const std::vector<std::string> &table_names,
                 const std::unordered_map<std::string, std::vector<std::string>> &table_to_column_map,
                 const std::unordered_map<std::string, std::string> &column_alias_map);

    public:
        void debug() const;
        void init() const;
        void execute() const;
        std::shared_ptr<ContextMemory> get_context_memory();
        std::shared_ptr<DataStore> get_datastore();
        void set_first_operator(const std::shared_ptr<Operator> &);

    private:
        const std::vector<std::string> _tables;
        std::shared_ptr<Operator> _first_operator;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _datastore;
    };

} // namespace VFEngine


#endif
