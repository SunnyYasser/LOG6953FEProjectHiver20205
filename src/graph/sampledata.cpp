//
// Created by sunny on 9/13/24.
//

#include "include/sampledata.hh"
namespace VFEngine {
    void populate_sample_data(const std::unique_ptr<AdjList[]> &_fwd_adj_list,
                              const std::unique_ptr<AdjList[]> &_bwd_adj_list) {

        // Forward adjacency list (fwd)
        _fwd_adj_list[0] = AdjList(0); // Empty list
        _fwd_adj_list[1] = AdjList(3); // List with 3 values
        _fwd_adj_list[1]._values[0] = 2;
        _fwd_adj_list[1]._values[1] = 3;
        _fwd_adj_list[1]._values[2] = 4;
        _fwd_adj_list[2] = AdjList(2); // List with 2 values
        _fwd_adj_list[2]._values[0] = 3;
        _fwd_adj_list[2]._values[1] = 4;
        _fwd_adj_list[3] = AdjList(1); // List with 1 value
        _fwd_adj_list[3]._values[0] = 4;
        _fwd_adj_list[4] = AdjList(0); // Empty list

        // Backward adjacency list (bwd)
        _bwd_adj_list[0] = AdjList(0); // Empty list
        _bwd_adj_list[1] = AdjList(0); // Empty list
        _bwd_adj_list[2] = AdjList(1); // List with 1 value
        _bwd_adj_list[2]._values[0] = 2;
        _bwd_adj_list[3] = AdjList(2); // List with 2 values
        _bwd_adj_list[3]._values[0] = 1;
        _bwd_adj_list[3]._values[1] = 2;
        _bwd_adj_list[4] = AdjList(3); // List with 3 values
        _bwd_adj_list[4]._values[0] = 1;
        _bwd_adj_list[4]._values[1] = 2;
        _bwd_adj_list[4]._values[2] = 3;
    }

} // namespace VFEngine
