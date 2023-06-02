set(SOURCES test_map.cpp)
add_executable(test_map ${SOURCES})
target_link_libraries(test_map PRIVATE parlay)
