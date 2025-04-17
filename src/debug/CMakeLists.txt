add_library(libdebug STATIC memory_debug.cpp
        operator_debug.cpp
        parser_debug.cpp)
target_include_directories(libdebug PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libdebug PRIVATE libutils PRIVATE liboperator PRIVATE libgraph PRIVATE libparser)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif()
