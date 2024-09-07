//
// Created by sunny on 9/4/24.
//

#ifndef VFENGINE_STATE_HASH_HH
#define VFENGINE_STATE_HASH_HH

#include "state.hh"
namespace VFEngine {
    class State;
}

struct StateSharedPtrHash {
    std::size_t operator()(const std::shared_ptr<VFEngine::State> &s) const {
        return std::hash<VFEngine::State*>{}(s.get());
    }
};

struct StateSharedPtrEqual {
    bool operator()(const std::shared_ptr<VFEngine::State>& lhs, const std::shared_ptr<VFEngine::State>& rhs) const {
        return lhs.get() == rhs.get();
    }
};

#endif
