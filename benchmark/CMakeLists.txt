set(SOURCES test_sets.cpp)

set(STRUCT_DIR ${PROJECT_SOURCE_DIR}/structures)
set(FLOCK_BENCH "arttree" "blockleaftree" "btree" "avltree" "leaftree"
		"list" "list_onelock" "dlist"
		"hash" "hash_block")
set(FLOCK_PER_BENCH "arttree" "btree" "hash_block")

set(OTHER_LIST_DIR ${PROJECT_SOURCE_DIR}/ds)
set(OTHER_LIST_BENCH "harris_list" "harris_list_opt")

function(add_benchmark NAME STRUCT_NAME DEFS)
  add_executable(${NAME} ${SOURCES})
  target_compile_definitions(${NAME} PRIVATE ${DEFS})
  target_link_libraries(${NAME} PRIVATE flock)
  target_include_directories(${NAME} PRIVATE ${STRUCT_NAME})
endfunction()

foreach(bench ${FLOCK_BENCH})
  add_benchmark(${bench} ${STRUCT_DIR}/${bench} "")
  add_benchmark(${bench}_hl ${STRUCT_DIR}/${bench} HashLock)
endforeach()

foreach(bench ${FLOCK_PER_BENCH})
  add_benchmark(${bench}_per ${STRUCT_DIR}/${bench} Persistent)
  add_benchmark(${bench}_per_hl ${STRUCT_DIR}/${bench} "Persistent;HashLock")
endforeach()

foreach(bench ${OTHER_LIST_BENCH})
  add_benchmark(${bench} ${OTHER_LIST_DIR}/${bench} "")
endforeach()

