#include <iostream>
#include <sstream>
#include <random>
#include "include/graph.hh"
#include "../data/include/CSVIngestor.hh"

namespace VFEngine {
    template<typename T>
    Graph<T>::Graph(std::string filename) : _filename(std::move(filename)) {
    }

    template<typename T>
    void Graph<T>::add_node(T src, T dest) {
        _adjList[src].push_back(dest);
    }

    template<typename T>
    bool Graph<T>::build_graph() {
        if (!CSVIngestionEngine::can_open_file(_filename)) {
            return false;
        }

        std::ifstream csvfile;
        CSVIngestionEngine::process_file(_filename, csvfile);
        std::string line;

        while (std::getline(csvfile, line)) {
            std::string word;
            std::stringstream ss(line);
            std::vector<std::string> words;

            while (std::getline(ss, word, ' ')) {
                words.push_back(word);
            }

            int src = std::stoi(words[0]);
            int dest = std::stoi(words[1]);

            add_node(src, dest);
        }

        return true;
    }


    template<typename T>
    void Graph<T>::print_graph() {
        std::cout << "Printing Graph .........\n" << std::endl;
        std::cout << "Graph size = " << _adjList.size() << std::endl;
        for (auto &[src, nbrs]: _adjList) {
            for (auto &nbr: nbrs) {
                std::cout << src << " " << nbr << std::endl;
            }
        }
    }

    template<typename T>
    std::vector<T> Graph<T>::get_random_datalist() {
        int sz = _adjList.size();
        int idx = rand() % sz;

        while (_adjList[idx].empty()) ++idx;

        return _adjList[idx];
    }



    template class Graph<int>;
}