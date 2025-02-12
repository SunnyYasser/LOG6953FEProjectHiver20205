#ifndef VFENGINE_TESTPATHS_HH
#define VFENGINE_TESTPATHS_HH

const char *get_dataset_csv_path();
const char *get_dataset_serialized_data_reading_path();
const char *get_dataset_serialized_data_writing_path();
const char *get_memory_debug_path();
const char *get_operator_debug_path();

// Following only used for building the unit testcases
bool is_running_amazon0601();
bool is_running_google_web();
bool is_running_soc_epinions();
bool is_running_live_journal();

#endif
