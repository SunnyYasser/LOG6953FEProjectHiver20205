#include "../include/schema.hh"
//
// Created by sunny on 9/3/24.
//

namespace SampleDB {
    Schema::Schema(const std::vector<std::string> &columns) {
        for (const auto &column : columns) {
            _schema_map [column] = UNDEFINED;
        }
    }
}