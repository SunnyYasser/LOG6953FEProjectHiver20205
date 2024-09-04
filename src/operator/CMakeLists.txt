add_library(liboperator STATIC operator_definition.cpp scan_operator.cpp index_nested_loop_join_operator.cpp sink_operator.cpp operator_utils.cpp
        schema.cpp)
target_include_directories(liboperator PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(liboperator PRIVATE libmem PRIVATE libgraph PRIVATE libengine PRIVATE libutils)