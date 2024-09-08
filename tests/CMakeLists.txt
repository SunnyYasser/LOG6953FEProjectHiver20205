# CMakeLists.txt for tests

# Include directories to access project headers
include_directories(${CMAKE_SOURCE_DIR}/src/data)
include_directories(${CMAKE_SOURCE_DIR}/src/graph)
include_directories(${CMAKE_SOURCE_DIR}/src/operator)
include_directories(${CMAKE_SOURCE_DIR}/src/memory)
include_directories(${CMAKE_SOURCE_DIR}/src/engine)
include_directories(${CMAKE_SOURCE_DIR}/src/utils)


# Test 1
add_executable(test1 test1.cpp)
target_link_libraries(test1 PUBLIC libgraph liboperator libmem libengine libutils)
add_test(NAME test1 COMMAND test1)

# Test 2
add_executable(test2 test2.cpp)
target_link_libraries(test2 PUBLIC libgraph liboperator libmem libengine libutils)
add_test(NAME test2 COMMAND test2)

# Test 3
add_executable(test3 test3.cpp)
target_link_libraries(test3 PUBLIC libgraph liboperator libmem libengine libutils)
add_test(NAME test3 COMMAND test3)

# Test 4
add_executable(test4 test4.cpp)
target_link_libraries(test4 PUBLIC libgraph liboperator libmem libengine libutils)
add_test(NAME test4 COMMAND test4)
