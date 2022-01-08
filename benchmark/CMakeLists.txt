set(SOURCES test_sets.cpp test_sets.h zipfian.h parse_command_line.h)
set(BENCHMARKS "arttree" "blockleaftree" "btree" "dlist" "hash" "leaftree" "list")

set(STRUCTDIR ${PROJECT_SOURCE_DIR}/structures)
include_directories(${STRUCTDIR})

foreach(bench ${BENCHMARKS})
  add_executable(${bench} ${SOURCES})
  target_compile_options(${bench} PRIVATE -include ${STRUCTDIR}/${bench}.h)
  target_link_libraries(${bench} PRIVATE flock)
endforeach()

