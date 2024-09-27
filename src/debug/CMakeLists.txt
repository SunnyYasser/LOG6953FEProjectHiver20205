add_library(libdebug STATIC memory_debug.cpp
        operator_debug.cpp)
target_include_directories(libdebug PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libdebug)