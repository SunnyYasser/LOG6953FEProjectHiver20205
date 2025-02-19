cmake_minimum_required(VERSION 3.5)

project(MySampleDB
        VERSION 0.0.1
        LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set default number of runs for profiling tests
if(NOT DEFINED NUM_RUNS)
    set(NUM_RUNS 3)
endif()

# Enable CTest for running tests
enable_testing()

add_subdirectory(src/data)
add_subdirectory(src/graph)
add_subdirectory(src/operator)
add_subdirectory(src/memory)
add_subdirectory(src/engine)
add_subdirectory(src/debug)
add_subdirectory(src/utils)
add_subdirectory(src/parser)

find_package(Threads REQUIRED)

add_executable(mydb2 main.cpp)
target_link_libraries(mydb2 PUBLIC libparser libgraph liboperator libmem libengine libutils libdebug)

function(enable_optimizations target)
    option(STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS "Disable pointer aliasing" OFF)
    option(ENABLE_BRANCHLESS_SINK_PACKED "Enable branchless Sink Packed Impl." OFF)
    option(VECTOR_STATE_ARENA_ALLOCATOR "Enable arena alloc for vectors" OFF)
    option(MEMSET_TO_SET_VECTOR_SLICE "Use memset for vector slices" OFF)
    if (STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS)
        target_compile_definitions(${target} PRIVATE STORAGE_TO_VECTOR_MEMCPY_PTR_ALIAS)
    endif ()
    if (ENABLE_BRANCHLESS_SINK_PACKED)
            target_compile_definitions(${target} PRIVATE ENABLE_BRANCHLESS_SINK_PACKED)
    endif ()
    if (VECTOR_STATE_ARENA_ALLOCATOR)
        target_compile_definitions(${target} PRIVATE VECTOR_STATE_ARENA_ALLOCATOR)
    endif ()
    if (MEMSET_TO_SET_VECTOR_SLICE)
        target_compile_definitions(${target} PRIVATE MEMSET_TO_SET_VECTOR_SLICE)
    endif ()
endfunction()


# Enable debug or release-specific options
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(mydb2 PRIVATE MY_DEBUG)
    target_compile_options(mydb2 PRIVATE -fsanitize=address -fsanitize=leak -g3 -Wall -Wextra -Wpedantic -Werror)
    target_link_options(mydb2 PRIVATE -fsanitize=address -fsanitize=leak)
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(mydb2 PRIVATE -Ofast -march=native)
endif ()

enable_optimizations(mydb2)

# Export NUM_RUNS to be used by subdirectories
set(NUM_RUNS ${NUM_RUNS} CACHE STRING "Number of runs for profiling tests")

# Add the tests directory
add_subdirectory(tests)
