cmake_minimum_required(VERSION 3.5)

project(MySampleDB
        VERSION 0.0.1
        LANGUAGES CXX
)

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

# Enable debug-specific options
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(mydb2 PRIVATE MY_DEBUG)
    target_compile_options(mydb2 PRIVATE -fsanitize=address -fsanitize=leak)
    target_link_options(mydb2 PRIVATE -fsanitize=address -fsanitize=leak)
endif()

# Add the tests directory
add_subdirectory(tests)
