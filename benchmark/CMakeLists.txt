set(SOURCES test_sets.cpp test_sets.h zipfian.h parse_command_line.h)
set(FLOCK_BENCH "arttree" "blockleaftree" "btree" "avltree" "leaftree"
		"list" "list_onelock" "dlist"
		"hash" "hash_block")
set(FLOCK_PER_BENCH "arttree" "btree" "hash_block")

set(STRUCTDIR ${PROJECT_SOURCE_DIR}/structures)

foreach(bench ${FLOCK_BENCH})
  add_executable(${bench} ${SOURCES})
  target_compile_options(${bench} PRIVATE -include ${STRUCTDIR}/${bench}.h)
  target_link_libraries(${bench} PRIVATE flock)
  target_include_directories(${bench} PRIVATE ${STRUCTDIR})
endforeach()

foreach(bench ${FLOCK_PER_BENCH})
  add_executable(${bench}_per ${SOURCES})
  target_compile_options(${bench}_per PRIVATE -DPersistent -include ${STRUCTDIR}/${bench}.h)
  target_link_libraries(${bench}_per PRIVATE flock)
  target_include_directories(${bench}_per PRIVATE ${STRUCTDIR})
endforeach()

set(OTHER_LIST_BENCH "harris_list" "harris_list_opt")
set(OTHER_LIST_DIR ${PROJECT_SOURCE_DIR}/ds)

foreach(bench ${OTHER_LIST_BENCH})
  add_executable(${bench} ${SOURCES})
  target_compile_options(${bench} PRIVATE -include
    ${OTHER_LIST_DIR}/${bench}/harris_ll_adapter.h)
  target_link_libraries(${bench} PRIVATE flock)
endforeach()

