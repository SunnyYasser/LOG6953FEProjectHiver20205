#include "include/pipeline.hh"

namespace VFEngine {
    Pipeline::Pipeline(const std::vector<std::string> &columns,
                       const std::unordered_map<std::string, std::string> &column_alias_map) :
        _first_operator(nullptr) {
        _context_memory = std::make_shared<ContextMemory>();
        _datastore = std::make_shared<DataStore>(columns, column_alias_map);
    }

    void Pipeline::debug() const { return; }

    void Pipeline::execute() const { _first_operator->execute(); }

    void Pipeline::init() const { _first_operator->init(_context_memory, _datastore); }

    std::shared_ptr<ContextMemory> Pipeline::get_context_memory() { return _context_memory; }

    std::shared_ptr<DataStore> Pipeline::get_datastore() { return _datastore; }

    void Pipeline::set_first_operator(const std::shared_ptr<Operator> &first_operator) {
        _first_operator = first_operator;
    }

    const std::shared_ptr<Operator> &Pipeline::get_first_operator() { return _first_operator; }

} // namespace VFEngine
