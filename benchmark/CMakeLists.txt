set(SOURCES test_sets.cpp)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/runall
          DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/runtests
          DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/run_sanity_checks.py
          DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
	
set(STRUCT_DIR ${PROJECT_SOURCE_DIR}/structures)
set(FLOCK_BENCH "arttree" "btree" "list" "list_ro" "dlist"
		"hash_block" "hash_block_lf")
set(FLOCK_BENCH_RECORDED_ONCE "btree" "list_ro")

set(OTHER_LIST_DIR ${PROJECT_SOURCE_DIR}/ds)
set(OTHER_LIST_BENCH "harris_list" "harris_list_opt")

function(add_benchmark NAME STRUCT_NAME DEFS)
  add_executable(${NAME} ${SOURCES})
  target_compile_definitions(${NAME} PRIVATE ${DEFS})
  target_link_libraries(${NAME} PRIVATE flock)
  target_include_directories(${NAME} PRIVATE ${STRUCT_NAME})
endfunction()

foreach(bench ${FLOCK_BENCH_RECORDED_ONCE})
  add_benchmark(${bench}_ro ${STRUCT_DIR}/${bench} "Persistent;Recorded_Once")
endforeach()

foreach(bench ${OTHER_LIST_BENCH})
  add_benchmark(${bench} ${OTHER_LIST_DIR}/${bench} "")
endforeach()

foreach(bench ${FLOCK_BENCH})
  add_benchmark(${bench} ${STRUCT_DIR}/${bench} "")
  add_benchmark(${bench}_indirect ${STRUCT_DIR}/${bench} "Persistent;FullyIndirect")
  add_benchmark(${bench}_noshortcut ${STRUCT_DIR}/${bench} "Persistent;Simple;NoShortcut")
  add_benchmark(${bench}_simple ${STRUCT_DIR}/${bench} "Persistent;Simple")
  add_benchmark(${bench}_per ${STRUCT_DIR}/${bench} "Persistent")
  add_benchmark(${bench}_read_stamp ${STRUCT_DIR}/${bench} "Persistent;ReadStamp")
  add_benchmark(${bench}_write_stamp ${STRUCT_DIR}/${bench} "Persistent;WriteStamp")
  add_benchmark(${bench}_per_lock ${STRUCT_DIR}/${bench} "Persistent;NoHelp")
endforeach()


