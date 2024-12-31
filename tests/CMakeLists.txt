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

    # Define the test command with conditional profiling
    if (RUN_WITH_PROFILING)
        add_test(NAME ${name} COMMAND bash ${CMAKE_BINARY_DIR}/run_with_profiling.sh ${CMAKE_BINARY_DIR}/tests/${name} 3)
    else ()
        add_test(NAME ${name} COMMAND ${CMAKE_BINARY_DIR}/tests/${name})
    endif ()
endfunction()

# Add all tests
add_profiling_test(test1 test1.cpp)
add_profiling_test(test2 test2.cpp)
add_profiling_test(test3 test3.cpp)
add_profiling_test(test4 test4.cpp)
add_profiling_test(test5_test3_packed test5.cpp)
add_profiling_test(test6_test2_packed test6.cpp)
add_profiling_test(test7_test1_packed test7.cpp)
add_profiling_test(test8_test4_packed test8.cpp)
add_profiling_test(test9 test9.cpp)
add_profiling_test(test10_test9_packed test10.cpp)
add_profiling_test(test11_left_deep_packed test11_left_deep_packed.cpp)
add_profiling_test(test12_left_deep test12_left_deep.cpp)
add_profiling_test(test13_test10_reorder test13_packed.cpp)
add_profiling_test(test14_test10_reorder test14_packed.cpp)
add_profiling_test(test15_test10_reorder test15_packed.cpp)
add_profiling_test(test16_test11_reorder test16_packed.cpp)
add_profiling_test(test17_test11_reorder test17_packed.cpp)
add_profiling_test(test18_test9_reorder test18.cpp)
add_profiling_test(test19_test9_reorder test19.cpp)
add_profiling_test(test20_test9_reorder test20.cpp)
add_profiling_test(test21_test12_reorder test21.cpp)
add_profiling_test(test22_test12_reorder test22.cpp)
add_profiling_test(test23_4hop_packed_linear_ordering test23_4hop_packed.cpp)
add_profiling_test(test24_4hop_unpacked_linear_ordering test24_4hop_unpacked.cpp)
add_profiling_test(test25_4hop_unpacked_noop_linear_ordering test25_4hop_unpacked_noop.cpp)
add_profiling_test(test26_4hop_packed_non_linear_ordering test26_4hop_packed.cpp)
add_profiling_test(test27_4hop_unpacked_non_linear_ordering test27_4hop_unpacked.cpp)
add_profiling_test(test28_4hop_unpacked_noop_non_linear_ordering test28_4hop_unpacked_noop.cpp)
add_profiling_test(test29_4hop_packed_noop_linear_ordering test29_4hop_packed_noop.cpp)
add_profiling_test(test30_4hop_packed_noop_non_linear_ordering test30_4hop_packed_noop.cpp)





# Copy the profiling script to the build directory
configure_file(${CMAKE_SOURCE_DIR}/tests/run_with_profiling.sh ${CMAKE_BINARY_DIR}/run_with_profiling.sh COPYONLY)
