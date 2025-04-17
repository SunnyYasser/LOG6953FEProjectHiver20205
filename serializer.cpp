#include <chrono>
#include <iostream>
#include "src/engine/include/pipeline.hh"
#include "src/graph/include/datasource.hh"

std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> result;
    size_t start = 0, end;

    while ((end = str.find(delimiter, start)) != std::string::npos) {
        result.push_back(str.substr(start, end - start));
        start = end + 1;
    }
    result.push_back(str.substr(start));

    return result;
}

int execute(const std::string& dataset_path, const std::string& output_dir, const std::string& column_names) {
    std::cout << "Starting data processing..." << std::endl;

    const std::vector<std::string> column_names_vector = split(column_names, ',');

    std::cout << "Initializing data source table..." << std::endl;
    VFEngine::DataSourceTable table("R", column_names_vector, dataset_path);

    std::cout << "Writing data to disk at: " << output_dir << std::endl;
    table.write_table_as_data_on_disk(output_dir);

    std::cout << "Data processing completed." << std::endl;
    return 0;
}


int main(const int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <dataset_path> <output_directory> <column_names>" << std::endl;
        return 1;
    }

    return execute(argv[1], argv[2], argv[3]);
}