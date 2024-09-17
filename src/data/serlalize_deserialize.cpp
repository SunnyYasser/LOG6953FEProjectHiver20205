//
// Created by sunny on 8/30/24.
//
#include <cstdint>
#include <fstream>
#include <iostream>
#include <thread>
#include "../graph/include/datasource.hh"
#include "../utils/include/testpaths.hh"
#include "include/serialize_deserialize.hh"

namespace VFEngine {
    template<typename T>
    SerializeDeserialize<T>::SerializeDeserialize(const std::string &filename, const VFEngine::DataSourceTable *table) :
        _filename(filename), _table(table) {}

    template<typename T>
    void SerializeDeserialize<T>::serialize() const {
        const auto &fwd_adj_list = _table->get_fwd_adj_list();
        const auto &bwd_adj_list = _table->get_bwd_adj_list();
        const char *data_folder = get_amazon0601_serialized_data_writing_path();

        if (!data_folder) {
            std::cerr << "Failed to read path for serializing Amazon0601" << std::endl;
            return;
        }

        serialize_adj_list(data_folder, fwd_adj_list);
        serialize_adj_list(data_folder, bwd_adj_list, true);
    }

    template<typename T>
    void SerializeDeserialize<T>::serialize_adj_list(const std::string &data_folder,
                                                     const std::unique_ptr<AdjList[]> &adj_list, bool reverse) const {
        auto rows = _table->get_rows_size();
        size_t num_threads = std::min(static_cast<uint64_t>(std::thread::hardware_concurrency()),
                                      rows); // Ensure threads do not exceed rows
        const size_t chunk_size = rows / num_threads;
        std::vector<std::thread> threads;
        size_t remaining_rows = rows % num_threads;
        size_t start_row = 0;
        for (size_t i = 0; i < num_threads; ++i) {
            size_t end_row = start_row + chunk_size + (i < remaining_rows ? 1 : 0); // Spread remaining rows evenly
            std::string filename =
                    reverse ? data_folder + "/bwd_" + std::to_string(start_row) + "_" + std::to_string(end_row) + ".bin"
                            : data_folder + "/fwd_" + std::to_string(start_row) + "_" + std::to_string(end_row) +
                                      ".bin";
            threads.emplace_back(SerializeDeserialize<T>::serialize_chunk, std::ref(adj_list), start_row, end_row,
                                 filename);
            start_row = end_row;
        }

        // Wait for all threads to finish
        for (auto &t: threads) {
            t.join();
        }
    }

    template<typename T>
    void SerializeDeserialize<T>::serialize_chunk(const std::unique_ptr<AdjList[]> &adj_list, const size_t &start_row,
                                                  const size_t &end_row, const std::string &filename) {
        std::ofstream outFile(filename, std::ios::binary);
        if (!outFile) {
            std::cerr << "Error opening file for writing: " << filename << std::endl;
            return;
        }

        for (size_t i = start_row; i < end_row; ++i) {
            const auto &row_size = adj_list[i]._size;
            const auto &row_values = adj_list[i]._values;

            // Write the size of the current row
            outFile.write(reinterpret_cast<const char *>(&row_size), sizeof(size_t));
            // Write the elements of the current row
            outFile.write(reinterpret_cast<const char *>(row_values), row_size * sizeof(uint64_t));
        }

        outFile.close();
    }


    template<typename T>
    void SerializeDeserialize<T>::deserialize() const {
        const auto &fwd_adj_list = _table->get_fwd_adj_list();
        const auto &bwd_adj_list = _table->get_bwd_adj_list();
        const char *data_folder = get_amazon0601_serialized_data_reading_path();

        if (!data_folder) {
            std::cerr << "Failed to read path for deserializing Amazon0601" << std::endl;
            return;
        }

        deserialize_adj_list(data_folder, fwd_adj_list);
        deserialize_adj_list(data_folder, bwd_adj_list, true);
    }

    template<typename T>
    void SerializeDeserialize<T>::deserialize_adj_list(const std::string &data_folder,
                                                       const std::unique_ptr<AdjList[]> &adj_list, bool reverse) const {
        auto rows = _table->get_rows_size();
        size_t num_threads = std::min(static_cast<uint64_t>(std::thread::hardware_concurrency()),
                                      rows); // Ensure threads do not exceed rows
        const size_t chunk_size = rows / num_threads;
        std::vector<std::thread> threads;
        size_t remaining_rows = rows % num_threads;
        size_t start_row = 0;

        for (size_t i = 0; i < num_threads; ++i) {
            size_t end_row = start_row + chunk_size + (i < remaining_rows ? 1 : 0); // Spread remaining rows evenly
            std::string filename =
                    reverse ? data_folder + "/bwd_" + std::to_string(start_row) + "_" + std::to_string(end_row) + ".bin"
                            : data_folder + "/fwd_" + std::to_string(start_row) + "_" + std::to_string(end_row) +
                                      ".bin";
            threads.emplace_back(SerializeDeserialize<T>::deserialize_chunk, std::ref(adj_list), start_row, end_row,
                                 filename);
            start_row = end_row;
        }

        // Wait for all threads to finish
        for (auto &t: threads) {
            t.join();
        }
    }

    template<typename T>
    void SerializeDeserialize<T>::deserialize_chunk(const std::unique_ptr<AdjList[]> &adj_list, const size_t &start_row,
                                                    const size_t &end_row, const std::string &filename) {
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs) {
            std::cerr << "Error opening file for writing: " << filename << std::endl;
            return;
        }

        for (size_t i = start_row; i < end_row; ++i) {
            size_t list_size;
            ifs.read(reinterpret_cast<char *>(&list_size), sizeof(std::size_t));
            AdjList new_list(list_size);
            ifs.read(reinterpret_cast<char *>(new_list._values), list_size * sizeof(uint64_t));
            adj_list[i] = std::move(new_list);
        }

        ifs.close();
    }

    template class SerializeDeserialize<uint64_t>;
} // namespace VFEngine
