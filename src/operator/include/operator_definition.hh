#ifndef VFENGINE_OPERATOR_DEFINITION_HH
#define VFENGINE_OPERATOR_DEFINITION_HH

#include <memory>
#include <string>
#include "../../graph/include/datastore.hh"
#include "../../memory/include/context.hh"
#include "operator_types.hh"
#include "schema.hh"

namespace VFEngine {
    class Operator {
    public:
        Operator();
        Operator(const Operator &) = delete;
        explicit  Operator(const std::shared_ptr<Operator> &next_operator);
        virtual ~Operator() = default;

        [[nodiscard]] std::string get_uuid() const;

        [[nodiscard]] std::string get_operator_info() const;

        std::shared_ptr<Operator> get_next_operator();

        [[nodiscard]] virtual operator_type_t get_operator_type() const;

        virtual void execute() = 0;

        virtual void debug() = 0;

        virtual void init(const std::shared_ptr<ContextMemory> &, const std::shared_ptr<DataStore> &) = 0;

    private:
        static std::string create_uuid();

        std::string _uuid;
        std::shared_ptr<Operator> _next_operator;
        operator_type_t _operator_type;
    };
} // namespace VFEngine

#endif
