add_library(libmem STATIC context.cpp state.cpp vector.cpp)
target_include_directories(libmem PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libmem PUBLIC libgraph)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif()
if(VECTOR_STATE_ARENA_ALLOCATOR)
    add_compile_definitions(VECTOR_STATE_ARENA_ALLOCATOR)
endif()

if(MEMSET_TO_SET_VECTOR_SLICE)
    add_compile_definitions(MEMSET_TO_SET_VECTOR_SLICE)
endif()

