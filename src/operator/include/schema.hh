//
// Created by sunny on 9/3/24.
//

#ifndef SAMPLE_DB_SCHEMA_HH
#define SAMPLE_DB_SCHEMA_HH

#include <string>
#include <unordered_map>
#include <vector>

namespace SampleDB {
    enum SchemaType { UNDEFINED = 0, UNFLAT = 1, FLAT = 2 };

    class Schema {
    public:
        Schema() = delete;
        Schema(const Schema &) = delete;
        Schema &operator=(const Schema &) = delete;
        explicit Schema(const std::vector<std::string> &);
        std::unordered_map<std::string, SchemaType> _schema_map;
    };
} // namespace SampleDB

#endif // SAMPLE_DB_SCHEMA_HH
