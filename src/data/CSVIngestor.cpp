//
// Created by Sunny on 13-06-2024.
//
#include <iostream>
#include <sstream>
#include "include/CSVIngestor.hh"

namespace VFEngine {
    CSVIngestionEngine &CSVIngestionEngine::get_reader_engine() {
        static CSVIngestionEngine engine;
        return engine;
    }

    bool CSVIngestionEngine::can_open_file(const std::string &filename) {
        std::ifstream file(filename);
        const bool success = file.is_open();

        if (success) {
            file.close();
        }

        return success;
    }

    void CSVIngestionEngine::process_file(const std::string &filename, std::ifstream& file) {
        file.open(filename);
    }

    void CSVIngestionEngine::print_data(const std::string &filename) {
        std::ifstream file(filename);
        std::string src, dest;
        while (file >> src >> dest) {
            std::cout << src << " " << dest << std::endl;
        }

        file.close();
    }
}