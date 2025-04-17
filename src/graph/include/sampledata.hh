//
// Created by sunny on 9/13/24.
//

#ifndef VFENGINE_SAMPLEDATA_HH
#define VFENGINE_SAMPLEDATA_HH

#include "adjlist.hh"
namespace VFEngine {
    void populate_sample_data(const std::unique_ptr<AdjList[]> &fwd_adj_list,
                              const std::unique_ptr<AdjList[]> &bwd_adj_list);
    inline uint64_t DEFAULT_MAX_ID_VALUE {4};
}
#endif
