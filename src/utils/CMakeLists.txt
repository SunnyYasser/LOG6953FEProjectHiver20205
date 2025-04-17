add_library(libutils STATIC debug_enabled.cpp
        testpath.cpp)
target_include_directories(libutils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif()
