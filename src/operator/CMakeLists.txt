add_library(liboperator STATIC operator_definition.cpp scan_operator.cpp index_nested_loop_join_operator.cpp sink_operator.cpp operator_utils.cpp
        index_nested_loop_join_to_1_operator.cpp
        index_nested_loop_join_packed_operator.cpp
        sink_packed_operator.cpp sink_no_op.cpp sink_packed_post_order_operator.cpp hardcoded_linear_plan_sink_packed.cpp
        sink_packed_min_operator.cpp cascade_selection_operator.cpp sink_failure_prop.cpp
        scan_failure_prop_operator.cpp
)
target_include_directories(liboperator PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(liboperator PRIVATE libmem PRIVATE libgraph PRIVATE libengine PRIVATE libutils)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif ()

if (STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS)
    add_compile_definitions(STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS)
endif ()

if (VECTOR_STATE_ARENA_ALLOCATOR)
    add_compile_definitions(VECTOR_STATE_ARENA_ALLOCATOR)
endif ()


