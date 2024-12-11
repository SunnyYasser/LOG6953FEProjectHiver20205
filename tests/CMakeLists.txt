# CMakeLists.txt for tests

# Include directories to access project headers
include_directories(${CMAKE_SOURCE_DIR}/src/data)
include_directories(${CMAKE_SOURCE_DIR}/src/graph)
include_directories(${CMAKE_SOURCE_DIR}/src/operator)
include_directories(${CMAKE_SOURCE_DIR}/src/memory)
include_directories(${CMAKE_SOURCE_DIR}/src/engine)
include_directories(${CMAKE_SOURCE_DIR}/src/utils)
include_directories(${CMAKE_SOURCE_DIR}/src/parser)


# Test 1
add_executable(test1 test1.cpp)
target_link_libraries(test1 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test1 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test1 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test1 COMMAND test1)

# Test 2
add_executable(test2 test2.cpp)
target_link_libraries(test2 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test2 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test2 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test2 COMMAND test2)

# Test 3
add_executable(test3 test3.cpp)
target_link_libraries(test3 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test3 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test3 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test3 COMMAND test3)

# Test 4
add_executable(test4 test4.cpp)
target_link_libraries(test4 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test4 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test4 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test4 COMMAND test4)

# Test 5
add_executable(test5 test5.cpp)
target_link_libraries(test5 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test5 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test5 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test5 COMMAND test5)

# Test 6
add_executable(test6 test6.cpp)
target_link_libraries(test6 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test6 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test6 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test6 COMMAND test6)

# Test 7
add_executable(test7 test7.cpp)
target_link_libraries(test7 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test7 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test7 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test7 COMMAND test7)

# Test 8
add_executable(test8 test8.cpp)
target_link_libraries(test8 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test8 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test8 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test8 COMMAND test8)

# Test 9
add_executable(test9 test9.cpp)
target_link_libraries(test9 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test9 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test9 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test9 COMMAND test9)

# Test 10
add_executable(test10 test10.cpp)
target_link_libraries(test10 PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test10 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test10 PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test10 COMMAND test10)

# Test 11
add_executable(test11_left_deep_packed test11_left_deep_packed.cpp)
target_link_libraries(test11_left_deep_packed PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test11_left_deep_packed PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test11_left_deep_packed PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test11_left_deep_packed COMMAND test11_left_deep_packed)

# Test 12
add_executable(test12_left_deep test12_left_deep.cpp)
target_link_libraries(test12_left_deep PUBLIC libgraph liboperator libmem libengine libutils libparser)
target_compile_options(test12_left_deep PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
target_link_options(test12_left_deep PRIVATE -fsanitize=address PRIVATE -fsanitize=leak)
add_test(NAME test12_left_deep COMMAND test12_left_deep)
