add_library(libparser STATIC query_parser.cpp
        factorized_tree.cpp
        factorized_tree_element.cpp)
target_include_directories(libparser PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libparser PRIVATE libmem PRIVATE libgraph PRIVATE liboperator PRIVATE libdebug)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif()
