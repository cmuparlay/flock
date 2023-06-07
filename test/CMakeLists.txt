set(SOURCES test_map.cpp)

function(add_benchmark NAME LINKS)
  add_executable(${NAME} ${SOURCES})
  target_link_libraries(${NAME} PRIVATE parlay ${LINKS})
  target_include_directories(${NAME} PRIVATE structures/${NAME})
endfunction()

add_benchmark(flock_hash "")
add_benchmark(flock_grow "")
add_benchmark(std "")
add_benchmark(std_sharded "")
add_benchmark(libcuckoo "")

add_subdirectory(abseil-cpp)
add_benchmark(abseil absl::flat_hash_map)
add_benchmark(abseil_sharded absl::flat_hash_map)

find_package(TBB)
add_benchmark(tbb TBB::tbb)







