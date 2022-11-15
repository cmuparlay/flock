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
  add_benchmark(${bench}_simple_ro ${STRUCT_DIR}/${bench} "Persistent;Simple_Recorded_Once")
  add_benchmark(${bench}_ro_rs ${STRUCT_DIR}/${bench} "Persistent;Recorded_Once;ReadStamp")
  add_benchmark(${bench}_ro_ns ${STRUCT_DIR}/${bench} "Persistent;Recorded_Once;NoIncStamp")
  add_benchmark(${bench}_ro_ws ${STRUCT_DIR}/${bench} "Persistent;Recorded_Once;WriteStamp")
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
  add_benchmark(${bench}_per_rs ${STRUCT_DIR}/${bench} "Persistent;ReadStamp")
  add_benchmark(${bench}_per_ns ${STRUCT_DIR}/${bench} "Persistent;NoIncStamp")
  add_benchmark(${bench}_per_ws ${STRUCT_DIR}/${bench} "Persistent;WriteStamp")
  add_benchmark(${bench}_per_ls ${STRUCT_DIR}/${bench} "Persistent;LazyStamp")
  add_benchmark(${bench}_simple_rs ${STRUCT_DIR}/${bench} "Persistent;Simple;ReadStamp")
  add_benchmark(${bench}_simple_ns ${STRUCT_DIR}/${bench} "Persistent;Simple;NoIncStamp")
  add_benchmark(${bench}_simple_ws ${STRUCT_DIR}/${bench} "Persistent;Simple;WriteStamp")
  add_benchmark(${bench}_per_lock ${STRUCT_DIR}/${bench} "Persistent;NoHelp")
endforeach()


