//
// Created by sunny on 8/30/24.
//
#include <fstream>
#include <iostream>
#include "include/serialize_deserialize.hh"

template<typename T>
Serialize_deserialize<T>::Serialize_deserialize(const std::string &filename) : _filename(filename) {}

template<typename T>
void Serialize_deserialize<T>::serializeVector(const std::vector<std::vector<T>> &vec) {
    std::ofstream ofs(_filename, std::ios::binary);
    if (!ofs) {
        std::cerr << "Error opening file for writing." << std::endl;
        return;
    }

    size_t rows = vec.size();
    ofs.write(reinterpret_cast<const char *>(&rows), sizeof(rows));
    if (rows > 0) {
        size_t cols = vec[0].size();
        ofs.write(reinterpret_cast<const char *>(&cols), sizeof(cols));

        for (const auto &row: vec) {
            ofs.write(reinterpret_cast<const char *>(row.data()), row.size() * sizeof(T));
        }
    }

    ofs.close();
}

template<typename T>
std::vector<std::vector<T>> Serialize_deserialize<T>::deserializeVector() {
    std::ifstream ifs(_filename, std::ios::binary);
    if (!ifs) {
        std::cerr << "Error opening file for reading." << std::endl;
        return {};
    }

    size_t rows;
    ifs.read(reinterpret_cast<char *>(&rows), sizeof(rows));

    if (rows <= 0) {
        std::cerr << "Error opening file for reading." << std::endl;
        return {};
    }

    std::vector<std::vector<int>> vec(rows);
    size_t cols;
    ifs.read(reinterpret_cast<char *>(&cols), sizeof(cols));

    for (size_t i = 0; i < rows; ++i) {
        vec[i].resize(cols);
        ifs.read(reinterpret_cast<char *>(vec[i].data()), cols * sizeof(T));
    }
    ifs.close();
    return vec;
}


template class Serialize_deserialize<int>;