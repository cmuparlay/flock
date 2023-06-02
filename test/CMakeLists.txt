set(SOURCES test_sets.cpp)
add_executable(test_sets ${SOURCES})
target_link_libraries(test_sets PRIVATE parlay)
