cmake_minimum_required(VERSION 3.1)

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
add_subdirectory(src/utils)


add_executable(mydb2 main.cpp)
target_link_libraries(mydb2 PUBLIC libgraph liboperator libmem libengine libutils)

# Add the tests directory
add_subdirectory(tests)
