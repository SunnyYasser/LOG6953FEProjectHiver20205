//
// Created by sunny on 8/30/24.
//

#include <cstdlib>


#include "include/testpaths.hh"

const char *get_amazon0601_csv_path() {
    const char *ev_val = getenv("VFENGINE_AMAZON0601_DATASET_PATH");
    return ev_val;
}

const char *get_amazon0601_serialized_data_reading_path() {
    const char *ev_val = getenv("VFENGINE_AMAZON0601_SERIALIZED_READ_PATH");
    return ev_val;
}

const char *get_amazon0601_serialized_data_writing_path() {
    const char *ev_val = getenv("VFENGINE_AMAZON0601_SERIALIZED_WRITE_PATH");
    return ev_val;
}
