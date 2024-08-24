add_library(libutils STATIC debug_enabled.cpp)
target_include_directories(libutils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
