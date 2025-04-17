#ifndef VFENGINE_GRAPH_HH
#define VFENGINE_GRAPH_HH

/*
Will integrate this code with CSVIngestor and
have an end to end file processing system. Will
also work on fast file reading, using boost memory
mapped files
 */


#include <string>
#include <unordered_map>
#include <vector>

namespace VFEngine {
    template<typename T>
    class Graph {
    public:
        Graph() = default;
        explicit Graph(std::string filename);
        Graph(const Graph<T> &) = delete;
        bool build_graph();
        void print_graph();
        std::vector<T> get_random_datalist();

    private:
        void add_node(T, T);
        std::unordered_map<T, std::vector<T>> _adjList;
        const std::string _filename;
    };
} // namespace VFEngine

#endif
