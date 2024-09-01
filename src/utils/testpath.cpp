//
// Created by sunny on 8/30/24.
//

#include "include/testpaths.hh"

const std::string get_fb_0edges_path () {
    const char* ev_val = getenv("FB_0EDGES_DATASET_PATH");
    if (ev_val == nullptr) {
        return "";
    }
    return std::string{ ev_val };
}