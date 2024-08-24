#include <random>
#include "include/operator_definition.hh"
#include "include/operator_utils.hh"
namespace SampleDB
{

    Operator::Operator(const std::vector<std::string>& columns, std::shared_ptr<Operator> next_operator) : _output_vector ({}), _input_vector ({}),
                                                                                                        _columns(columns), _operator_type (get_operator_type()),
                                                                                                        _next_operator(next_operator)

    {
        _uuid = create_uuid();
    }

    const std::string Operator::get_uuid() const
    {
        return _uuid;
    }

    const std::string Operator::get_operator_info () const
    {
        return get_operator_name_as_string (_operator_type, _uuid);
    }

    operator_type_t Operator::get_operator_type () const
    {
        return OP_GENERIC;
    }

    std::shared_ptr <Operator> Operator::get_next_operator ()
    {
        return _next_operator;
    }

    std::vector <std::string> Operator::get_attributes () const
    {
        return _columns;
    }

    const std::string Operator::create_uuid()
    {

        static std::random_device dev;
        static std::mt19937 rng(dev());

        std::uniform_int_distribution<int> dist(0, 15);

        constexpr auto v = "0123456789abcdef";
        const uint8_t dash[] = {0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0};

        std::string res;
        for (int i = 0; i < 16; i++)
        {
            if (dash[i])
                res += "-";
            res += v[dist(rng)];
            res += v[dist(rng)];
        }

        return res;
    }
}