set(SOURCES bst_bench.cpp llcode_adapter.h ../benchmark/test_sets.h ../benchmark/zipfian.h ../benchmark/parse_command_line.h)
set(SET_BENCH_DIR ${CMAKE_CURRENT_SOURCE_DIR}/setbench)

set(SET_BENCH "ellen_ext_bst_lf" "brown_ext_chromatic_lf" "bronson_pext_bst_occ" "srivastava_abtree_pub" "drachsler_pext_bst_lock" "natarajan_ext_bst_lf")

# A hack to get a list of include directories
execute_process(COMMAND find ${SET_BENCH_DIR}/common -type d OUTPUT_VARIABLE INCL_STRING)
string(REPLACE "\n" ";" INCL_LIST ${INCL_STRING})

set(DEBRA_OPTIONS -DRECLAIM_TYPE=debra -DUSE_TREE_STATS -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.4  -DMEMORY_STATS=if\(0\) -DMEMORY_STATS2=if\(0\) -DDEBRA_ORIGINAL_FREE)

foreach(bench ${SET_BENCH})
  add_executable(${bench} ${SOURCES})
  target_link_libraries(${bench} PRIVATE flock)
  target_include_directories(${bench} PRIVATE ${SET_BENCH_DIR}/ds/${bench})
  target_include_directories(${bench} PRIVATE ${INCL_LIST})
  target_compile_options(${bench} PRIVATE -fpermissive ${DEBRA_OPTIONS})
  set_target_properties(${bench} PROPERTIES LINKER_LANGUAGE CXX)
endforeach()
