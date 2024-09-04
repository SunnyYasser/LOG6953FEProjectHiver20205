//
// Created by sunny on 9/4/24.
//

#ifndef SAMPLEDB_STATE_HASH_HH
#define SAMPLEDB_STATE_HASH_HH

#include "state.hh"
namespace SampleDB {
    class State;
}

struct StateSharedPtrHash {
    std::size_t operator()(const std::shared_ptr<SampleDB::State> &s) const {
        return std::hash<SampleDB::State*>{}(s.get());
    }
};

struct StateSharedPtrEqual {
    bool operator()(const std::shared_ptr<SampleDB::State>& lhs, const std::shared_ptr<SampleDB::State>& rhs) const {
        return lhs.get() == rhs.get();
    }
};

#endif //SAMPLEDB_STATE_HASH_HH
