//
// Created by sunny on 9/13/24.
//

#include "include/sampledata.hh"

#include <cstdlib>
namespace VFEngine {
    static void populate_sample_data1(const std::unique_ptr<AdjList[]> &_fwd_adj_list,
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
        _bwd_adj_list[2]._values[0] = 1;
        _bwd_adj_list[3] = AdjList(2); // List with 2 values
        _bwd_adj_list[3]._values[0] = 1;
        _bwd_adj_list[3]._values[1] = 2;
        _bwd_adj_list[4] = AdjList(3); // List with 3 values
        _bwd_adj_list[4]._values[0] = 1;
        _bwd_adj_list[4]._values[1] = 2;
        _bwd_adj_list[4]._values[2] = 3;
    }

    static void populate_sample_data_one2one_state_sharing(const std::unique_ptr<AdjList[]> &_fwd_adj_list,
                                                           const std::unique_ptr<AdjList[]> &_bwd_adj_list) {

        // Forward adjacency list (fwd)
        _fwd_adj_list[0] = AdjList(1);
        _fwd_adj_list[0]._values[0] = 1;
        _fwd_adj_list[1] = AdjList(1);
        _fwd_adj_list[1]._values[0] = 2;
        _fwd_adj_list[2] = AdjList(1);
        _fwd_adj_list[2]._values[0] = 3;
        _fwd_adj_list[3] = AdjList(1);
        _fwd_adj_list[3]._values[0] = 4;
        _fwd_adj_list[4] = AdjList(1);
        _fwd_adj_list[4]._values[0] = 5;
        _fwd_adj_list[5] = AdjList(1);
        _fwd_adj_list[5]._values[0] = 6;
        _fwd_adj_list[6] = AdjList(1);
        _fwd_adj_list[6]._values[0] = 7;
        _fwd_adj_list[7] = AdjList(0);

        // Backward adjacency list (bwd)
        _bwd_adj_list[0] = AdjList(0);
        _bwd_adj_list[1] = AdjList(1);
        _bwd_adj_list[1]._values[0] = 0;
        _bwd_adj_list[2] = AdjList(1);
        _bwd_adj_list[2]._values[0] = 1;
        _bwd_adj_list[3] = AdjList(1);
        _bwd_adj_list[3]._values[0] = 2;
        _bwd_adj_list[4] = AdjList(1);
        _bwd_adj_list[4]._values[0] = 3;
        _bwd_adj_list[5] = AdjList(1);
        _bwd_adj_list[5]._values[0] = 4;
        _bwd_adj_list[6] = AdjList(1);
        _bwd_adj_list[6]._values[0] = 5;
        _bwd_adj_list[7] = AdjList(1);
        _bwd_adj_list[7]._values[0] = 6;
    }

    void populate_sample_data(const std::unique_ptr<AdjList[]> &fwd_adj_list,
                              const std::unique_ptr<AdjList[]> &bwd_adj_list) {
        if (getenv("VFENGINE_ENABLE_STATE_SHARING"))
            populate_sample_data_one2one_state_sharing(fwd_adj_list, bwd_adj_list);
        else
            populate_sample_data1(fwd_adj_list, bwd_adj_list);
    }


} // namespace VFEngine
