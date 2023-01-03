set(SOURCES test_sets.cpp)

set(BENCH_FILES "runtests" "run_sanity_checks.py")

function(copy_bench_file NAME)
  file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/${NAME}
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endfunction()

foreach(file ${BENCH_FILES})
  copy_bench_file(${file})
endforeach()
	
set(STRUCT_DIR ${PROJECT_SOURCE_DIR}/structures)
set(FLOCK_BENCH "leaftree" "blockleaftree" "arttree" "btree" "dlist" "list" "list_onelock" "hash_block" "hash")
set(FLOCK_BENCH_USE_CAS "hash_block")

set(OTHER_LIST_DIR ${PROJECT_SOURCE_DIR}/ds)
set(OTHER_LIST_BENCH "harris_list" "harris_list_opt")

function(add_benchmark NAME STRUCT_NAME DEFS)
  add_executable(${NAME} ${SOURCES})
  target_compile_definitions(${NAME} PRIVATE ${DEFS})
  target_link_libraries(${NAME} PRIVATE flock)
  target_include_directories(${NAME} PRIVATE ${STRUCT_NAME})
endfunction()

foreach(bench ${OTHER_LIST_BENCH})
  add_benchmark(${bench} ${OTHER_LIST_DIR}/${bench} "")
endforeach()

foreach(bench ${FLOCK_BENCH})
  add_benchmark(${bench} ${STRUCT_DIR}/${bench} "")
  add_benchmark(${bench}_nohelp ${STRUCT_DIR}/${bench} "NoHelp")
endforeach()

# None of these need helping
foreach(bench ${FLOCK_BENCH_USE_CAS})
  add_benchmark(${bench}_lf ${STRUCT_DIR}/${bench} "NoHelp;UseCAS")
  add_benchmark(${bench}_lf_nohelp ${STRUCT_DIR}/${bench} "UseCAS")
endforeach()
