#include "include/state.hh"
#include <fstream>

namespace VFEngine {
    State::State(const int32_t &size) : _pos(-1), _size(size) {
    }

    void State::print_debug_info(std::ofstream& logfile) {
        logfile << "[STATE pos]: " << _pos << std::endl;
        logfile << "[STATE size]: " << _size << std::endl;
    }

}
