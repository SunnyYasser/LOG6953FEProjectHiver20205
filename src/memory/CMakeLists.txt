add_library(libmem STATIC context.cpp state.cpp vector.cpp)
target_include_directories(libmem PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libmem PUBLIC libgraph)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif()
if(DISABLE_PTR_ALIAS)
    add_compile_definitions(DISABLE_PTR_ALIAS)
endif()

if(ARENA_ALLOCATOR)
    add_compile_definitions(ARENA_ALLOCATOR)
endif()

if(REMOVE_MEMSET)
    add_compile_definitions(REMOVE_MEMSET)
endif()

