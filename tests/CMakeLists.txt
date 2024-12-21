# Include directories to access project headers
include_directories(${CMAKE_SOURCE_DIR}/src/data)
include_directories(${CMAKE_SOURCE_DIR}/src/graph)
include_directories(${CMAKE_SOURCE_DIR}/src/operator)
include_directories(${CMAKE_SOURCE_DIR}/src/memory)
include_directories(${CMAKE_SOURCE_DIR}/src/engine)
include_directories(${CMAKE_SOURCE_DIR}/src/utils)
include_directories(${CMAKE_SOURCE_DIR}/src/parser)

# Define a function to add tests with profiling enabled
function(add_profiling_test name source)
    add_executable(${name} ${source})
    target_link_libraries(${name} PUBLIC libgraph liboperator libmem libengine libutils libparser)
    target_compile_options(${name} PRIVATE -fsanitize=address -fsanitize=leak)
    target_link_options(${name} PRIVATE -fsanitize=address -fsanitize=leak)
    add_test(NAME ${name} COMMAND bash ${CMAKE_BINARY_DIR}/run_with_profiling.sh ${CMAKE_BINARY_DIR}/tests/${name})
endfunction()

# Add all tests
add_profiling_test(test1 test1.cpp)
add_profiling_test(test2 test2.cpp)
add_profiling_test(test3 test3.cpp)
add_profiling_test(test4 test4.cpp)
add_profiling_test(test5 test5.cpp)
add_profiling_test(test6 test6.cpp)
add_profiling_test(test7 test7.cpp)
add_profiling_test(test8 test8.cpp)
add_profiling_test(test9 test9.cpp)
add_profiling_test(test10 test10.cpp)
add_profiling_test(test11_left_deep_packed test11_left_deep_packed.cpp)
add_profiling_test(test12_left_deep test12_left_deep.cpp)

# Copy the profiling script to the build directory
configure_file(${CMAKE_SOURCE_DIR}/tests/run_with_profiling.sh ${CMAKE_BINARY_DIR}/run_with_profiling.sh COPYONLY)
