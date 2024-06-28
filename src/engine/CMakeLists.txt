add_library(libengine STATIC pipeline.cpp)
target_include_directories(libengine PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(libengine PUBLIC liboperator PUBLIC libmem PUBLIC libgraph)