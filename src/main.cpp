#include <iostream>
#include <memory>
#include <unistd.h>
#include "graph/include/graph.hh"
#include "operator/include/scan_operator.hh"
#include "operator/include/index_nested_loop_join_operator.hh"
#include "operator/include/sink_operator.hh"
#include "engine/include/pipeline.hh"
#include "utils/include/debug_enabled.h"

std::shared_ptr<SampleDB::Operator> create_operator_plan(const std::unordered_map<std::string, std::vector<std::string>>& table_to_column_map)
{
    auto sink = std::shared_ptr<SampleDB::Operator>(new SampleDB::Sink(table_to_column_map));

    std::vector<std::pair<std::string, std::string>> join_pair {{"R1", "a"}, {"R2", "b"}};
    auto extend = std::shared_ptr<SampleDB::Operator>(new SampleDB::IndexNestedLoopJoin(join_pair, sink));

    std::vector<std::pair<std::string, std::string>> scan_attribute {{"R1", "a"}};
    auto scan = std::shared_ptr<SampleDB::Operator>(new SampleDB::Scan(scan_attribute, extend));

    return scan;
}

void pipeline_example()
{

    std::vector<std::string> table_names {"R1", "R2"};
    std::unordered_map<std::string, std::vector<std::string>> table_to_column_map {
        {"R1", {"a", "b" }},
        {"R2", {"a", "b"}}
    };


    auto pipeline = std::make_shared<SampleDB::Pipeline>(table_names, table_to_column_map);

    auto first_op = create_operator_plan(table_to_column_map);
    pipeline->set_first_operator (first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    // pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();

    pipeline->clear();
}

int main()
{
    if (std::getenv ("GETCHAR_IN_MAIN")) {
        enable_debug();
        std::cout << "Current process id: " << ::getpid () << std::endl;
        do
        {
            std::cout << '\n' << "Press a key to continue...";
        } while (std::cin.get() != '\n');

    }
    std::cout << "Hello\n";
    pipeline_example();

    return 0;
}