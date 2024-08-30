//
// Created by sunny on 8/30/24.
//

#ifndef SAMPLE_DB_SERIALIZE_DESERIALIZE_HH
#define SAMPLE_DB_SERIALIZE_DESERIALIZE_HH

#include <string>
#include <vector>

template<typename T>
class Serialize_deserialize {
public:
    Serialize_deserialize(const std::string &);
    Serialize_deserialize() = delete;
    Serialize_deserialize(const Serialize_deserialize &) = delete;
    Serialize_deserialize &operator=(const Serialize_deserialize &) = delete;
    Serialize_deserialize &&operator=(Serialize_deserialize &&) = delete;

    void serializeVector(const std::vector<std::vector<T>>&);
    std::vector<std::vector<T>> deserializeVector();

private:
    const std::string _filename;
};
#endif // SAMPLE_DB_SERIALIZE_DESERIALIZE_HH
