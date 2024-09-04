#include "include/operator_definition.hh"
#include <random>
#include "include/operator_utils.hh"

namespace SampleDB {
    static Vector temp; // to initialize the _input_vector reference
    static std::string table_name{"TEMP TABLE"};

    Operator::Operator(const std::shared_ptr<Schema> &schema) :
        _output_vector(temp), _input_vector(temp), _table_name("R"), _operator_type(Operator::get_operator_type()),
        _schema(schema), _next_operator(nullptr) {
        _uuid = create_uuid();
    }

    Operator::Operator(const std::string &table_name, const std::shared_ptr<Schema> &schema,
                       const std::shared_ptr<Operator> &next_operator) :
        _output_vector(temp), _input_vector(temp), _table_name(table_name),
        _operator_type(Operator::get_operator_type()), _schema(schema), _next_operator(next_operator) {
        _uuid = create_uuid();
    }

    std::string Operator::get_uuid() const { return _uuid; }

    std::string Operator::get_operator_info() const { return get_operator_name_as_string(_operator_type, _uuid); }

    operator_type_t Operator::get_operator_type() const { return OP_GENERIC; }

    std::shared_ptr<Operator> Operator::get_next_operator() { return _next_operator; }

    std::string Operator::get_table_name() const { return _table_name; }

    std::string Operator::create_uuid() {
        static std::random_device dev;
        static std::mt19937 rng(dev());

        std::uniform_int_distribution<int> dist(0, 15);

        const uint8_t dash[] = {0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0};

        std::string res;
        for (unsigned char i: dash) {
            constexpr auto v = "0123456789abcdef";
            if (i)
                res += "-";
            res += v[dist(rng)];
            res += v[dist(rng)];
        }

        return res;
    }
} // namespace SampleDB
