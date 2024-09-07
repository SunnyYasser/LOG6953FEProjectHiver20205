//
// Created by sunny on 8/30/24.
//

#ifndef VFENGINE_SERIALIZE_DESERIALIZE_HH
#define VFENGINE_SERIALIZE_DESERIALIZE_HH

#include <string>
#include <vector>

template<typename T>
class SerializeDeserialize {
public:
    explicit SerializeDeserialize(const std::string &filename);
    SerializeDeserialize() = delete;
    SerializeDeserialize(const SerializeDeserialize &) = delete;
    SerializeDeserialize &operator=(const SerializeDeserialize &) = delete;
    SerializeDeserialize &&operator=(SerializeDeserialize &&) = delete;

    void serializeVector(const std::vector<std::vector<T>>&);
    std::vector<std::vector<T>> deserializeVector();

private:
    const std::string _filename;
};
#endif
