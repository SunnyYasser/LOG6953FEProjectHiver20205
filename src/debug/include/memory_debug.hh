#ifndef VFENGINE_GRAPH_DEBUG_HH
#define VFENGINE_GRAPH_DEBUG_HH

#include <vector>
#include <memory>

namespace VFEngine{
    class AdjList;
    class MemoryDebugUtility
    {
        public:
        static void print_adj_list(std::vector<std::vector<uint64_t>> &adj_list, uint64_t max_id, bool reverse = false);
        static void print_adj_list(const std::unique_ptr<AdjList[]> &adj_list, uint64_t max_id, bool reverse = false);
    };
}

#endif
