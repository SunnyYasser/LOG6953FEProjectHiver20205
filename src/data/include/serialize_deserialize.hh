//
// Created by sunny on 8/30/24.
//

#ifndef VFENGINE_SERIALIZE_DESERIALIZE_HH
#define VFENGINE_SERIALIZE_DESERIALIZE_HH

#include <string>
namespace VFEngine {
    template<typename T>
    class SerializeDeserialize {
    public:
        SerializeDeserialize(const std::string &filename, const VFEngine::DataSourceTable *table);
        SerializeDeserialize() = delete;
        SerializeDeserialize(const SerializeDeserialize &) = delete;
        SerializeDeserialize &operator=(const SerializeDeserialize &) = delete;
        SerializeDeserialize &&operator=(SerializeDeserialize &&) = delete;

        void serialize() const;
        void deserialize() const;

    private:
        const std::string _filename;
        const DataSourceTable *_table;
        void deserialize_adj_list(const std::string &folder, const std::unique_ptr<AdjList[]> &adj_list,
                                  bool reverse = false) const;
        void serialize_adj_list(const std::string &folder, const std::unique_ptr<AdjList[]> &adj_list,
                                bool reverse = false) const;
        static void serialize_chunk(const std::unique_ptr<AdjList[]> &adj_list, const size_t &start_row,
                                    const size_t &end_row, const std::string &filename);
        static void deserialize_chunk(const std::unique_ptr<AdjList[]> &adj_list, const size_t &start_row,
                                      const size_t &end_row, const std::string &filename);
    };
} // namespace VFEngine

#endif
