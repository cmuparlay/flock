set(SOURCES test_sets.cpp test_sets.h zipfian.h parse_command_line.h)
set(FLOCK_BENCH "arttree" "blockleaftree" "btree" "dlist" "hash" "leaftree" "list")

set(STRUCTDIR ${PROJECT_SOURCE_DIR}/structures)

foreach(bench ${FLOCK_BENCH})
  add_executable(${bench} ${SOURCES})
  target_compile_options(${bench} PRIVATE -include ${STRUCTDIR}/${bench}.h)
  target_link_libraries(${bench} PRIVATE flock)
  target_include_directories(${bench} PRIVATE ${STRUCTDIR})
endforeach()

set(OTHER_LIST_BENCH "harris_list" "harris_list_opt")
set(OTHER_LIST_DIR ${PROJECT_SOURCE_DIR}/ds)

foreach(bench ${OTHER_LIST_BENCH})
  add_executable(${bench} ${SOURCES})
  target_compile_options(${bench} PRIVATE -include
    ${OTHER_LIST_DIR}/${bench}/harris_ll_adapter.h)
  target_link_libraries(${bench} PRIVATE flock)
endforeach()

