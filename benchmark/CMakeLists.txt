set(SOURCES test_sets.cpp)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/runall
          DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/runtests
          DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/run_sanity_checks.py
          DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
	
set(STRUCT_DIR ${PROJECT_SOURCE_DIR}/structures)
set(FLOCK_BENCH "arttree" "btree" "list" "list_ro" "dlist" "hash_block")
set(FLOCK_BENCH_NO_LOCKS "hash_block_lf")
set(FLOCK_BENCH_RECORDED_ONCE "btree" "list_ro")

set(OTHER_LIST_DIR ${PROJECT_SOURCE_DIR}/ds)
set(OTHER_LIST_BENCH "harris_list" "harris_list_opt")

function(add_benchmark NAME STRUCT_NAME DEFS)
  add_executable(${NAME} ${SOURCES})
  target_compile_definitions(${NAME} PRIVATE ${DEFS})
  target_link_libraries(${NAME} PRIVATE verlib)
  target_include_directories(${NAME} PRIVATE ${STRUCT_NAME})
endfunction()

foreach(bench ${FLOCK_BENCH_RECORDED_ONCE})
  add_benchmark(${bench}_ro_ls ${STRUCT_DIR}/${bench} "Versioned;Recorded_Once;LazyStamp")
endforeach()

foreach(bench ${OTHER_LIST_BENCH})
  add_benchmark(${bench} ${OTHER_LIST_DIR}/${bench} "")
endforeach()

foreach(bench ${FLOCK_BENCH})
  add_benchmark(${bench} ${STRUCT_DIR}/${bench} "")
  add_benchmark(${bench}_indirect_ls ${STRUCT_DIR}/${bench} "Versioned;FullyIndirect;LazyStamp")
  add_benchmark(${bench}_noshortcut_ls ${STRUCT_DIR}/${bench} "Versioned;Simple;NoShortcut;LazyStamp")
  add_benchmark(${bench}_simple_ls ${STRUCT_DIR}/${bench} "Versioned;Simple;LazyStamp")
  add_benchmark(${bench}_ver_ss ${STRUCT_DIR}/${bench} "Versioned")
  add_benchmark(${bench}_ver_rs ${STRUCT_DIR}/${bench} "Versioned;ReadStamp")
  add_benchmark(${bench}_ver_ns ${STRUCT_DIR}/${bench} "Versioned;NoIncStamp")
  add_benchmark(${bench}_ver_ws ${STRUCT_DIR}/${bench} "Versioned;WriteStamp")
  add_benchmark(${bench}_ver_ls ${STRUCT_DIR}/${bench} "Versioned;LazyStamp")
  add_benchmark(${bench}_ver_lock_ls ${STRUCT_DIR}/${bench} "Versioned;NoHelp;LazyStamp")
endforeach()

# None of these need helping
foreach(bench ${FLOCK_BENCH_NO_LOCKS})
  add_benchmark(${bench} ${STRUCT_DIR}/${bench} "")
  add_benchmark(${bench}_indirect_ls ${STRUCT_DIR}/${bench} "NoHelp;Versioned;FullyIndirect;LazyStamp")
  add_benchmark(${bench}_noshortcut_ls ${STRUCT_DIR}/${bench} "NoHelp;Versioned;Simple;NoShortcut;LazyStamp")
  add_benchmark(${bench}_simple_ls ${STRUCT_DIR}/${bench} "NoHelp;Versioned;Simple;LazyStamp")
  add_benchmark(${bench}_ver_ss ${STRUCT_DIR}/${bench} "NoHelp;Versioned")
  add_benchmark(${bench}_ver_rs ${STRUCT_DIR}/${bench} "NoHelp;Versioned;ReadStamp")
  add_benchmark(${bench}_ver_ns ${STRUCT_DIR}/${bench} "NoHelp;Versioned;NoIncStamp")
  add_benchmark(${bench}_ver_ws ${STRUCT_DIR}/${bench} "NoHelp;Versioned;WriteStamp")
  add_benchmark(${bench}_ver_ls ${STRUCT_DIR}/${bench} "NoHelp;Versioned;LazyStamp")
  add_benchmark(${bench}_ver_lock_ls ${STRUCT_DIR}/${bench} "NoHelp;Versioned;LazyStamp")
endforeach()


