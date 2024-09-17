//
// Created by sunny on 9/15/24.
//
#include "include/modes.hh"

#include <cstdlib>

int is_hot_run_mode_enabled() {
    auto result_str = getenv("VFENGINE_ENABLE_HOT_RUN");
    return result_str ? atoi(result_str) : 0;
}

int is_cold_run_mode_enabled() {
    auto result_str = getenv("VFENGINE_ENABLE_COLD_RUN");
    return result_str ? atoi(result_str) : 0;
}

int is_serializing_mode_disabled() {
    auto result_str = getenv("VFENGINE_DISABLE_SERIALIZE");
    return result_str ? atoi(result_str) : 0;
}

int is_deserializing_mode_disabled() {
    auto result_str = getenv("VFENGINE_DISABLE_DESERIALIZE");
    return result_str ? atoi(result_str) : 0;
}
