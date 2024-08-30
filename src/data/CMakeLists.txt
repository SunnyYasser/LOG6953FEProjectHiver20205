add_library(libdata STATIC CSVIngestor.cpp)
target_include_directories(libdata PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)