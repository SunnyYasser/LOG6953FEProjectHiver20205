#ifndef VFENGINE_GRAPH_DEBUG_HH
#define VFENGINE_GRAPH_DEBUG_HH

#include <memory>
#include <vector>

namespace VFEngine {
    class AdjList;
    class MemoryDebugUtility {
    public:
        static void print_adj_list(const std::unique_ptr<std::vector<uint64_t>[]> &adj_list, uint64_t max_id,
                                   bool reverse = false);
        static void print_adj_list(const std::unique_ptr<AdjList[]> &adj_list, uint64_t max_id, bool reverse = false);
        static void print_serialize_deseerialize_time(ulong duration);
    };
} // namespace VFEngine

#endif
