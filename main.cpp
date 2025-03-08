// #include <cstring>
// #include <hardcoded_linear_plan_sink_packed.hh>
// #include <iostream>
// #include <memory>
// #include <sink_no_op.hh>
// #include <sink_packed_min_operator.hh>
// #include <sink_packed_operator.hh>
// #include <sys/resource.h>
// #include <unistd.h>
// #include "src/engine/include/pipeline.hh"
// #include "src/operator/include/index_nested_loop_join_operator.hh"
// #include "src/operator/include/relation_types.hh"
// #include "src/operator/include/scan_operator.hh"
// #include "src/operator/include/sink_operator.hh"
// #include "src/parser/include/factorized_tree_element.hh"
// #include "src/parser/include/query_parser.hh"
// #include "src/utils/include/debug_enabled.hh"


// std::shared_ptr<VFEngine::Operator> create_operator_plan1() {

//     const std::vector<std::string> columns = {"a", "b"};
//     const std::vector<std::pair<std::string, VFEngine::SchemaType>> schema{{"a", VFEngine::FLAT},
//                                                                            {"b", VFEngine::UNFLAT}};

//     // Sink
//     auto sink = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Sink>(schema));


//     // Join1
//     const std::string ip_attribute1 = "a";
//     const std::string op_attribute1 = "b";
//     bool is_join_index_fwd1 = true;
//     VFEngine::RelationType relation_type1 = VFEngine::MANY_TO_MANY;
//     auto extend1 = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::IndexNestedLoopJoin>(
//             ip_attribute1, op_attribute1, is_join_index_fwd1, relation_type1, sink));

//     // Scan
//     const std::string scan_attribute = "a";
//     auto scan = std::static_pointer_cast<VFEngine::Operator>(std::make_shared<VFEngine::Scan>(scan_attribute,
//     extend1));

//     return scan;
// }

// void pipeline_example1() {
//     std::vector<std::string> column_names{"src", "dest"};
//     std::unordered_map<std::string, std::vector<std::string>> table_to_column_map{{"R", {"src", "dest"}}};
//     const std::string datalog = "Q = R(a, b)";
//     const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}};

//     auto pipeline = std::make_shared<VFEngine::Pipeline>(column_names, column_alias_map);

//     auto first_op = create_operator_plan1();
//     pipeline->set_first_operator(first_op);

//     std::cout << "Begin init.............\n";
//     pipeline->init();

//     std::cout << "Begin debug.............\n";
//     pipeline->debug();

//     std::cout << "Begin execute.............\n";
//     pipeline->execute();

//     std::cout << "count (*) R(a, b) = " << VFEngine::Sink::get_total_row_size_if_materialized() << std::endl;
//     std::cout << "Execute calls for different operators" << std::endl;

//     const std::vector<std::string> operator_names{"SCAN", "INLJ", "SINK"};
//     int idx = 0;

//     while (first_op) {
//         std::cout << operator_names[idx++] << " " << first_op->get_uuid() << " : " <<
//         first_op->get_exec_call_counter()
//                   << std::endl;
//         first_op = first_op->get_next_operator();
//     }
// }

// void enable_component_debug() {
//     if (std::getenv("GETCHAR_IN_OPERATOR")) {
//         enable_operator_debug();
//     }

//     if (std::getenv("GETCHAR_IN_MEMORY")) {
//         enable_memory_debug();
//     }

//     if (std::getenv("GETCHAR_IN_PARSER")) {
//         enable_parser_debug();
//     }
// }

// std::shared_ptr<VFEngine::FactorizedTreeElement> create_ftree() {
//     const auto root_b = std::make_shared<VFEngine::FactorizedTreeElement>("b");
//     const auto node_a = std::make_shared<VFEngine::FactorizedTreeElement>("a");
//     const auto node_c = std::make_shared<VFEngine::FactorizedTreeElement>("c");
//     const auto node_d = std::make_shared<VFEngine::FactorizedTreeElement>("d");
//     const auto node_e = std::make_shared<VFEngine::FactorizedTreeElement>("e");

//     root_b->_children.push_back(node_a);
//     root_b->_children.push_back(node_d);
//     node_a->_children.push_back(node_c);
//     node_c->_children.push_back(node_e);

//     root_b->print_tree();
//     return root_b;
// }

// void parser_example() {
//     const std::string datalog = "a->b,b->c,c->d,d->e";
//     const std::vector<std::string> column_ordering = {"c", "b", "a", "d", "e"};
//     const std::unordered_map<std::string, std::string> column_alias_map{
//             {"a", "src"}, {"b", "src"}, {"c", "src"}, {"d", "src"}, {"e", "dest"}};


//     const std::vector<std::string> column_names{"src", "dest"};
//     const auto parser = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, true, VFEngine::SinkType::PACKED, column_names, column_alias_map);

//     const auto pipeline = parser->build_physical_pipeline();
//     pipeline->init();
//     pipeline->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::SinkPacked::get_total_row_size_if_materialized()
//               << std::endl;

//     const auto parser2 = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, false, VFEngine::SinkType::UNPACKED, column_names, column_alias_map);

//     const auto pipeline2 = parser2->build_physical_pipeline();
//     pipeline2->init();
//     pipeline2->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::Sink::get_total_row_size_if_materialized() <<
//     std::endl;

//     const auto parser3 = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, true, VFEngine::SinkType::NO_OP, column_names, column_alias_map);

//     const auto pipeline3 = parser3->build_physical_pipeline();
//     pipeline3->init();
//     pipeline3->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::SinkNoOp::get_total_row_size_if_materialized()
//               << std::endl;

//     const auto parser4 = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, false, VFEngine::SinkType::NO_OP, column_names, column_alias_map);

//     const auto pipeline4 = parser4->build_physical_pipeline();
//     pipeline4->init();
//     pipeline4->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::SinkNoOp::get_total_row_size_if_materialized()
//               << std::endl;


//     const auto &auto_gen_ftree1 = parser->create_factorized_tree();
//     const auto &auto_gen_ftree2 = parser2->create_factorized_tree();
//     const auto &auto_gen_ftree3 = parser3->create_factorized_tree();
//     const auto &auto_gen_ftree4 = parser4->create_factorized_tree();

//     // ftree->print_tree();
//     auto_gen_ftree1->print_tree();
//     auto_gen_ftree2->print_tree();
//     auto_gen_ftree3->print_tree();
//     auto_gen_ftree4->print_tree();
// }

// void parser_example2() {
//     const std::string datalog = "a->b";
//     const std::vector<std::string> column_ordering = {"a", "b"};
//     const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "dest"}};


//     const std::vector<std::string> column_names{"src", "dest"};
//     const auto parser = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, true, VFEngine::SinkType::PACKED, column_names, column_alias_map);

//     const auto pipeline = parser->build_physical_pipeline();
//     pipeline->init();
//     pipeline->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::SinkPacked::get_total_row_size_if_materialized()
//               << std::endl;

//     const auto parser2 = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, false, VFEngine::SinkType::UNPACKED, column_names, column_alias_map);

//     const auto pipeline2 = parser2->build_physical_pipeline();
//     pipeline2->init();
//     pipeline2->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::Sink::get_total_row_size_if_materialized() <<
//     std::endl;

//     const auto parser3 = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, true, VFEngine::SinkType::NO_OP, column_names, column_alias_map);

//     const auto pipeline3 = parser3->build_physical_pipeline();
//     pipeline3->init();
//     pipeline3->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::SinkNoOp::get_total_row_size_if_materialized()
//               << std::endl;

//     const auto parser4 = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, false, VFEngine::SinkType::NO_OP, column_names, column_alias_map);

//     const auto pipeline4 = parser4->build_physical_pipeline();
//     pipeline4->init();
//     pipeline4->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::SinkNoOp::get_total_row_size_if_materialized()
//               << std::endl;
// }

// void linear_sink_example() {
//     const std::string datalog = "a->b,b->c";
//     const std::vector<std::string> column_ordering = {"a", "b", "c"};
//     const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "src"}, {"c", "dest"}};
//     const std::vector<std::string> column_names{"src", "dest"};

//     const auto parser = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, true, VFEngine::SinkType::HARDCODED_LINEAR, column_names, column_alias_map);

//     const auto pipeline = parser->build_physical_pipeline();
//     pipeline->init();
//     pipeline->execute();

//     std::cout << "count (*) " << datalog << " = " <<
//     VFEngine::SinkLinearHardcoded::get_total_row_size_if_materialized()
//               << std::endl;


//     const auto parser2 = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, false, VFEngine::SinkType::UNPACKED, column_names, column_alias_map);

//     const auto pipeline2 = parser2->build_physical_pipeline();
//     pipeline2->init();
//     pipeline2->execute();

//     std::cout << "count (*) " << datalog << " = " << VFEngine::Sink::get_total_row_size_if_materialized() <<
//     std::endl;
// }

// void min_sink_example() {
//     const std::string datalog = "a->b,b->c,c->d,d->e";
//     const std::vector<std::string> column_ordering = {"a", "b", "c", "d", "e"};
//     const std::unordered_map<std::string, std::string> column_alias_map{
//             {"a", "src"}, {"b", "src"}, {"c", "src"}, {"d", "src"}, {"e", "dest"}};

//     // const std::string datalog = "a->b,b->c";
//     // const std::vector<std::string> column_ordering = {"a", "b", "c"};
//     // const std::unordered_map<std::string, std::string> column_alias_map{{"a", "src"}, {"b", "src"}, {"c",
//     "dest"}}; const std::vector<std::string> column_names{"src", "dest"};

//     const auto parser = std::make_unique<VFEngine::QueryParser>(
//             datalog, column_ordering, true, VFEngine::SinkType::PACKED_MIN, column_names, column_alias_map);

//     const auto pipeline = parser->build_physical_pipeline();
//     pipeline->init();
//     pipeline->execute();

//     const auto freqs = VFEngine::SinkPackedMin::get_min_values();
//     std::cout << "a" << " = " << freqs[0] << std::endl;
//     std::cout << "b" << " = " << freqs[1] << std::endl;
//     std::cout << "c" << " = " << freqs[2] << std::endl;
//     std::cout << "d" << " = " << freqs[3] << std::endl;
//     std::cout << "e" << " = " << freqs[4] << std::endl;
// }

// int main() {
//     if (std::getenv("GETCHAR_IN_MAIN")) {
//         enable_debug();
//         std::cout << "Current process id: " << ::getpid() << std::endl;
//         do {
//             std::cout << '\n' << "Press a key to continue...";
//         } while (std::cin.get() != '\n');
//     }

//     enable_component_debug();
//     // parser_example();
//     // parser_example2();
//     // linear_sink_example();
//     min_sink_example();

//     struct rusage usage;
//     // Get resource usage
//     if (getrusage(RUSAGE_SELF, &usage) == 0) {
//         double peak_memory_mb = usage.ru_maxrss / 1024.0;
//         printf("Peak Memory Usage: %.2f MB\n", peak_memory_mb);
//     } else {
//         perror("getrusage failed");
//     }

//     return 0;
// }

#include <chrono>
#include <iostream>
#include <memory>
#include "src/operator/include/sink_packed_operator.hh"
#include <sys/resource.h>
#include "src/engine/include/pipeline.hh"
#include "src/operator/include/sink_operator.hh"
#include "src/parser/include/query_parser.hh"

void print_column_ordering(const std::vector<std::string> &column_ordering) {
    std::cout << "COLUMN ORDERING: ";
    for (size_t i = 0; i < column_ordering.size(); i++) {
        std::cout << column_ordering[i];
        if (i != column_ordering.size() - 1) {
            std::cout << ", ";
        }
    }
    std::cout << std::endl;
}

inline void benchmark_barrier() {
    std::atomic_thread_fence(std::memory_order_seq_cst); // prevent hardware reordering
    asm volatile("" ::: "memory"); // prevent compiler reordering (only for gcc)
}

std::chrono::steady_clock::time_point exec_start_time;
std::chrono::steady_clock::time_point exec_end_time;

ulong run_pipeline(const std::string &dataset_path, const std::string &serialized_dataset_path,
                   const std::string &query, const std::vector<std::string> &column_ordering, const bool &is_packed) {
    std::vector<std::string> column_names{"src", "dest"};

    std::unordered_map<std::string, std::string> column_alias_map;
    for (int i = 0; i < static_cast<int>(column_ordering.size()); i++) {
        if (i == 0) {
            column_alias_map[column_ordering[i]] = column_names[0];
        } else {
            column_alias_map[column_ordering[i]] = column_names[1];
        }
    }

    print_column_ordering(column_ordering);
    const auto parser = std::make_unique<VFEngine::QueryParser>(
            query, column_ordering, is_packed, is_packed ? VFEngine::SinkType::PACKED : VFEngine::SinkType::UNPACKED,
            column_names, column_alias_map);

    VFEngine::DataSourceTable::set_dataset_path(dataset_path);
    VFEngine::DataSourceTable::set_serialized_dataset_path(serialized_dataset_path);
    const auto pipeline = parser->build_physical_pipeline();
    pipeline->init();

    benchmark_barrier();
    exec_start_time = std::chrono::steady_clock::now();

    pipeline->execute();

    benchmark_barrier();
    exec_end_time = std::chrono::steady_clock::now();

    auto first_op = pipeline->get_first_operator();

    std::vector<std::string> operator_names;
    operator_names.emplace_back("SCAN");
    for (int i = 0; i < static_cast<int>(column_ordering.size()) - 1; i++) {
        if (is_packed) {
            operator_names.push_back("INLJ_PACKED" + std::to_string(i + 1));
        } else {
            operator_names.push_back("INLJ" + std::to_string(i + 1));
        }
    }
    operator_names.emplace_back("SINK");

    int idx = 0;
    while (first_op) {
        std::cout << operator_names[idx++] << " " << first_op->get_uuid() << " : " << first_op->get_exec_call_counter()
                  << std::endl;
        first_op = first_op->get_next_operator();
    }

    return is_packed ? VFEngine::SinkPacked::get_total_row_size_if_materialized()
                     : VFEngine::Sink::get_total_row_size_if_materialized();
}

std::vector<std::string> split(const std::string &str, char delimiter) {
    std::vector<std::string> result;
    size_t start = 0, end;

    while ((end = str.find(delimiter, start)) != std::string::npos) {
        result.push_back(str.substr(start, end - start));
        start = end + 1;
    }
    result.push_back(str.substr(start));

    return result;
}

int execute(const std::string &dataset_path, const std::string &serialized_dataset_path, const std::string &query,
            const std::string &column_ordering, const ulong &expected_result, const bool &is_packed = false) {
    std::cout << "Executed Query: " << query << std::endl;
    const auto start = std::chrono::steady_clock::now();
    const std::vector<std::string> column_ordering_vector = split(column_ordering, ',');
    const auto actual_result =
            run_pipeline(dataset_path, serialized_dataset_path, query, column_ordering_vector, is_packed);
    const auto end = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    const auto exec_duration = std::chrono::duration_cast<std::chrono::microseconds>(exec_end_time - exec_start_time);

    struct rusage usage;
    // Get resource usage
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        double peak_memory_mb = usage.ru_maxrss / 1024.0;
        printf("Peak Memory Usage: %.2f MB\n", peak_memory_mb);
    } else {
        printf("Peak Memory Usage: %d MB\n", -1);
    }

    std::cout << "Total Time: " << duration.count() << " us" << std::endl;
    std::cout << "Execution Time: " << exec_duration.count() << " us" << std::endl;
    std::cout << "Actual Result: " << actual_result << std::endl;
    std::cout << "Expected Result: " << expected_result << std::endl;
    const auto is_valid = actual_result == expected_result;
    std::cout << "Is Valid: " << is_valid << std::endl;

    if (actual_result != expected_result) {
        std::cerr << "Execution failed: Expected " << expected_result << " but got " << actual_result << std::endl;
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 6 && argc != 7) {
        std::cerr << "Usage: " << argv[0]
                  << " <dataset_path> <serialized_dataset_path> <query> <column_ordering> <expected_result> [is_packed]"
                  << std::endl;
        return 1;
    }
    const bool is_packed = argc == 7 ? std::stoi(argv[6]) : false;

    return execute(argv[1], argv[2], argv[3], argv[4], std::stoul(argv[5]), is_packed);
}
