//
// Created by sunny on 8/30/24.
//

#include <cstdlib>


#include "include/testpaths.hh"

const char *get_dataset_csv_path() {
    const char *ev_val = getenv("VFENGINE_DATASET_PATH");
    return ev_val;
}

const char *get_dataset_serialized_data_reading_path() {
    const char *ev_val = getenv("VFENGINE_SERIALIZED_READ_PATH");
    return ev_val;
}

const char *get_dataset_serialized_data_writing_path() {
    const char *ev_val = getenv("VFENGINE_SERIALIZED_WRITE_PATH");
    return ev_val;
}

const char *get_memory_debug_path() { return "operator_debug_logs"; }

const char *get_operator_debug_path() { return "operator_debug_logs"; }

bool is_running_amazon0601() {
    const char *ev_val = getenv("VFENGINE_AMAZON0601");
    return ev_val != nullptr;
}

bool is_running_google_web() {
    const char *ev_val = getenv("VFENGINE_GOOGLE_WEB");
    return ev_val != nullptr;
}

bool is_running_soc_epinions() {
    const char *ev_val = getenv("VFENGINE_SOC_EPINIONS");
    return ev_val != nullptr;
}

bool is_running_live_journal() {
    const char *ev_val = getenv("VFENGINE_LIVE_JOURNAL");
    return ev_val != nullptr;
}
