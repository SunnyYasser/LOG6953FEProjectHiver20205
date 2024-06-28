add_library(libgraph STATIC graph.cpp datastore.cpp)
target_include_directories(libgraph PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libgraph PUBLIC libdata)