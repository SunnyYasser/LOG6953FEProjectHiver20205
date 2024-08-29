#include <iostream>
#include <memory>
#include <unistd.h>
#include "graph/include/graph.hh"
#include "operator/include/scan_operator.hh"
#include "operator/include/index_nested_loop_join_operator.hh"
#include "operator/include/sink_operator.hh"
#include "engine/include/pipeline.hh"
#include "utils/include/debug_enabled.h"

std::shared_ptr<SampleDB::Operator> create_operator_plan(
    const std::unordered_map<std::string, std::vector<std::string> > &table_to_column_map) {

    // Sink
    std::string sink_table = "R2";
    std::vector<std::string> sink_column {"b"};
    auto sink = std::static_pointer_cast<SampleDB::Operator>(
        std::make_shared<SampleDB::Sink>(sink_table, sink_column));

    // Join2
    std::vector<std::pair<std::string, std::string> > join_pair2{{"R1", "b"}, {"R2", "b"}};
    const auto left_table2 = join_pair2.front().first;
    const auto right_table2 = join_pair2.back().first;
    auto left_column2 = join_pair2.front().second;
    auto right_column2 = join_pair2.back().second;
    const std::vector<std::string> columns2{left_column2, right_column2};

    auto extend2 = std::static_pointer_cast<SampleDB::Operator>(
        std::make_shared<SampleDB::IndexNestedLoopJoin>(left_table2, right_table2, columns2, sink));


    // Join1
    std::vector<std::pair<std::string, std::string> > join_pair1{{"R1", "a"}, {"R1", "b"}};
    const auto left_table1 = join_pair1.front().first;
    const auto right_table1 = join_pair1.back().first;
    auto left_column1 = join_pair1.front().second;
    auto right_column1 = join_pair1.back().second;

    const std::vector<std::string> columns1{left_column1, right_column1};

    auto extend1 = std::static_pointer_cast<SampleDB::Operator>(
        std::make_shared<SampleDB::IndexNestedLoopJoin>(left_table1, right_table1, columns1, extend2));

    //Scan
    std::vector<std::pair<std::string, std::string> > scan_attribute{{"R1", "a"}};
    auto scan = std::static_pointer_cast<SampleDB::Operator>(std::make_shared<SampleDB::Scan>(scan_attribute, extend1));

    return scan;
}
std::shared_ptr<SampleDB::Operator> create_operator_plan2(
    const std::unordered_map<std::string, std::vector<std::string> > &table_to_column_map) {

    // Sink
    std::string sink_table = "R1";
    std::vector<std::string> sink_column {"b"};
    auto sink = std::static_pointer_cast<SampleDB::Operator>(
        std::make_shared<SampleDB::Sink>(sink_table, sink_column));

    // Join1
    std::vector<std::pair<std::string, std::string> > join_pair1{{"R1", "a"}, {"R1", "b"}};
    const auto left_table1 = join_pair1.front().first;
    const auto right_table1 = join_pair1.back().first;
    auto left_column1 = join_pair1.front().second;
    auto right_column1 = join_pair1.back().second;

    const std::vector<std::string> columns1{left_column1, right_column1};

    auto extend1 = std::static_pointer_cast<SampleDB::Operator>(
        std::make_shared<SampleDB::IndexNestedLoopJoin>(left_table1, right_table1, columns1, sink));

    //Scan
    std::vector<std::pair<std::string, std::string> > scan_attribute{{"R1", "a"}};
    auto scan = std::static_pointer_cast<SampleDB::Operator>(std::make_shared<SampleDB::Scan>(scan_attribute, extend1));

    return scan;
}

void pipeline_example1() {
    std::vector<std::string> table_names{"R1", "R2"};
    std::unordered_map<std::string, std::vector<std::string> > table_to_column_map{
        {"R1", {"a", "b"}},
        {"R2", {"a", "b"}}
    };


    auto pipeline = std::make_shared<SampleDB::Pipeline>(table_names, table_to_column_map);

    auto first_op = create_operator_plan(table_to_column_map);
    pipeline->set_first_operator(first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();

    pipeline->clear();
}

void pipeline_example2() {
    std::vector<std::string> table_names{"R1", "R2"};
    std::unordered_map<std::string, std::vector<std::string> > table_to_column_map{
            {"R1", {"a", "b"}},
            {"R2", {"a", "b"}}
    };


    auto pipeline = std::make_shared<SampleDB::Pipeline>(table_names, table_to_column_map);

    auto first_op = create_operator_plan2(table_to_column_map);
    pipeline->set_first_operator(first_op);

    std::cout << "Begin init.............\n";
    pipeline->init();

    std::cout << "Begin debug.............\n";
    pipeline->debug();

    std::cout << "Begin execute.............\n";
    pipeline->execute();

    pipeline->clear();
}

int main() {
    if (std::getenv("GETCHAR_IN_MAIN")) {
        enable_debug();
        std::cout << "Current process id: " << ::getpid() << std::endl;
        do {
            std::cout << '\n' << "Press a key to continue...";
        } while (std::cin.get() != '\n');
    }

    pipeline_example1();
    //pipeline_example2();

    return 0;
}
