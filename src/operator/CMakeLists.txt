add_library(liboperator STATIC operator_definition.cpp scan_operator.cpp index_nested_loop_join_operator.cpp sink_operator.cpp operator_utils.cpp
        index_nested_loop_join_to_1_operator.cpp
        index_nested_loop_join_packed_operator.cpp
        sink_packed_operator.cpp sink_no_op.cpp
)
target_include_directories(liboperator PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(liboperator PRIVATE libmem PRIVATE libgraph PRIVATE libengine PRIVATE libutils)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif ()

if (DISABLE_PTR_ALIAS)
    add_compile_definitions(DISABLE_PTR_ALIAS)
endif ()

if (ARENA_ALLOCATOR)
    add_compile_definitions(ARENA_ALLOCATOR)
endif ()

if (REMOVE_MEMSET)
    add_compile_definitions(REMOVE_MEMSET)
endif ()

if (ENABLE_BRANCHLESS_SINK_PACKED)
    add_compile_definitions(ENABLE_BRANCHLESS_SINK_PACKED)
endif ()


