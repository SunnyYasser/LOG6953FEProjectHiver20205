//
// Created by Sunny on 13-06-2024.
//

#ifndef VFENGINE_CSV_INGESTOR_HH
#define VFENGINE_CSV_INGESTOR_HH

#include <fstream>
#include <string>

namespace VFEngine {
    class CSVIngestionEngine {
    public:
        static CSVIngestionEngine &get_reader_engine();
        static bool can_open_file(const std::string &filename);
        static void process_file(const std::string &filename, std::ifstream &file);
        static void print_data(const std::string &filename);
        CSVIngestionEngine(const CSVIngestionEngine &) = delete;
        CSVIngestionEngine &operator=(const CSVIngestionEngine &) = delete;
        CSVIngestionEngine(CSVIngestionEngine &&) = delete;
        CSVIngestionEngine &operator=(CSVIngestionEngine &&) = delete;

    private:
        CSVIngestionEngine() = default;

    private:
        const std::string _filename;
    };
} // namespace VFEngine


#endif
