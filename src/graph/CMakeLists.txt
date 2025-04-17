add_library(libgraph STATIC graph.cpp datastore.cpp
        datasource.cpp
        adjlist.cpp
        sampledata.cpp arena_allocator.cpp)
target_include_directories(libgraph PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libgraph PRIVATE libdata PRIVATE libutils PRIVATE libdebug)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(MY_DEBUG)
endif ()
if(VECTOR_STATE_ARENA_ALLOCATOR)
    add_compile_definitions(VECTOR_STATE_ARENA_ALLOCATOR)
endif()
