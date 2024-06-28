//
// Created by Sunny on 13-06-2024.
//

#ifndef SAMPLE_DB_CSV_INGESTOR_HH
#define SAMPLE_DB_CSV_INGESTOR_HH

#include <string>
#include <fstream>

namespace SampleDB {
    class CSVIngestionEngine {
    public:
        static CSVIngestionEngine &get_reader_engine();
        static bool can_open_file(const std::string &);
        static void process_file(const std::string &, std::ifstream &);
        static void print_data(const std::string &);
        CSVIngestionEngine(const CSVIngestionEngine &) = delete;
        CSVIngestionEngine &operator=(const CSVIngestionEngine &) = delete;
        CSVIngestionEngine(CSVIngestionEngine &&) = delete;
        CSVIngestionEngine &operator=(CSVIngestionEngine &&) = delete;

    private:
        CSVIngestionEngine() = default;

    private:
        const std::string _filename;
    };
}


#endif