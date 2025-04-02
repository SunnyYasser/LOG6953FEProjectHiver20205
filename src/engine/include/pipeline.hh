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
        Pipeline();
        Pipeline(const std::vector<std::string> &columns,
                 const std::unordered_map<std::string, std::string> &column_alias_map);

    public:
        void debug() const;
        void init() const;
        void execute() const;
        std::shared_ptr<ContextMemory> get_context_memory();
        std::shared_ptr<DataStore> get_datastore();
        void set_first_operator(const std::shared_ptr<Operator> &);
        void set_last_operator(const std::shared_ptr<Operator> &last_operator);
        void set_datastore(const std::shared_ptr<DataStore> &datastore);
        const std::shared_ptr<Operator> &get_first_operator();
        const std::shared_ptr<Operator> &get_last_operator();

    private:
        const std::vector<std::string> _columns;
        std::shared_ptr<Operator> _first_operator;
        std::shared_ptr<Operator> _last_operator;
        std::shared_ptr<ContextMemory> _context_memory;
        std::shared_ptr<DataStore> _datastore;
    };

} // namespace VFEngine


#endif
