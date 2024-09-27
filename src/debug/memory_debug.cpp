#include "include/memory_debug.hh"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include "../graph/include/adjlist.hh"
#include "../utils/include/debug_enabled.hh"
#include "../utils/include/testpaths.hh"

namespace VFEngine {

    void MemoryDebugUtility::print_adj_list(std::vector<std::vector<uint64_t>> &adj_list, uint64_t max_id,
                                            bool reverse) {
        if (!is_memory_debug_enabled()) {
            return;
        }

        const auto data_writing_folder = get_amazon0601_serialized_data_writing_path();
        std::string folder;

        if (!data_writing_folder) {
            folder = "debug_logs";
        } else {
            folder = data_writing_folder;
        }
        std::filesystem::create_directories(folder);

        std::ofstream file;
        if (reverse) {
            const auto &filename = folder + "/bwd_adj_list.txt";
            file.open(filename);
        } else {
            const auto &filename = folder + "/fwd_adj_list.txt";
            file.open(filename);
        }
        if (!file.is_open()) {
            std::cerr << "Could not open for dumping logs" << std::endl;
            return;
        }

        uint64_t edges = 0;
        file << "Number nodes : " << adj_list.size() << std::endl;
        for (uint64_t idx = 0; idx <= max_id; ++idx) {
            if (adj_list[idx].empty()) {
                continue;
            }
            file << idx << ",";
            if (adj_list[idx].size() > 1) {
                file << "\"";
            }
            edges += adj_list[idx].size();
            std::sort(adj_list[idx].begin(), adj_list[idx].end());

            for (size_t j = 0; j < adj_list[idx].size(); j++) {
                file << adj_list[idx][j];
                if (j != adj_list[idx].size() - 1) {
                    file << ",";
                }
            }

            if (adj_list[idx].size() > 1) {
                file << "\"";
            }

            file << std::endl;
        }
        file << "Number edges : " << edges << std::endl;
        file.close();
    }

    void MemoryDebugUtility::print_adj_list(const std::unique_ptr<AdjList[]> &adj_list, uint64_t max_id, bool reverse) {
        if (!is_memory_debug_enabled()) {
            return;
        }

        const auto data_writing_folder = get_amazon0601_serialized_data_writing_path();
        std::string folder;

        if (!data_writing_folder) {
            folder = "debug_logs";
        } else {
            folder = data_writing_folder;
        }

        std::filesystem::create_directories(folder);

        std::ofstream file;
        if (reverse) {
            const auto &filename = folder + "/deserialized_bwd_adj_list.txt";
            file.open(filename);
        } else {
            const auto &filename = folder + "/deserialized_fwd_adj_list.txt";
            file.open(filename);
        }
        if (!file.is_open()) {
            std::cerr << "Could not open for dumping logs" << std::endl;
            return;
        }

        uint64_t edges = 0;
        file << "Number nodes : " << max_id + 1 << std::endl;

        for (uint64_t idx = 0; idx <= max_id; ++idx) {
            if (adj_list[idx]._size == 0) {
                continue;
            }
            file << idx << ",";
            if (adj_list[idx]._size > 1) {
                file << "\"";
            }

            uint64_t adj_list_size = adj_list[idx]._size;
            edges += adj_list_size;

            std::vector<uint64_t> values_copy(adj_list_size, 0);
            std::copy(adj_list[idx]._values, adj_list[idx]._values + adj_list_size, values_copy.begin());
            std::sort(values_copy.begin(), values_copy.end());

            for (size_t j = 0; j < adj_list_size; j++) {
                file << values_copy[j];
                if (j != adj_list_size - 1) {
                    file << ",";
                }
            }

            if (adj_list_size > 1) {
                file << "\"";
            }

            file << std::endl;
        }
        file << "Number edges : " << edges << std::endl;
        file.close();
    }

} // namespace VFEngine
