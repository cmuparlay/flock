

DFLAGS = -DRECLAIM_TYPE=debra -DUSE_TREE_STATS -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.4  -DMEMORY_STATS=if\(0\) -DMEMORY_STATS2=if\(0\) -DDEBRA_ORIGINAL_FREE
CFLAGS = -std=c++17 -mcx16 -march=native -O3 -DHOMEGROWN -pthread -DNDEBUG  -fpermissive
CFLAGS_DEBUG = -std=c++17 -mcx16 -march=native -fsanitize=undefined -fsanitize=address -static-libasan -fno-omit-frame-pointer -g -DHOMEGROWN -pthread 
IFLAGS = -Isetbench -I../include -I../include/flock `find setbench/common -type d | sed s/^/-I/`
LFLAGS = -Lsetbench/lib  -L/usr/local/lib -lpthread -ldl -ljemalloc

all: bronson_bst_bench ellen_bst_bench chromatic_bst_bench drachsler_bst_bench  natarajan_bst_bench  srivastava_abtree_pub_bench srivastava_abtree_pub_bench_debug chromatic_bst_bench_debug

ellen_bst_bench:
	g++ ./bst_bench.cpp -o bin/ellen_bst_bench -Isetbench/ds/ellen_ext_bst_lf $(DFLAGS) $(CFLAGS) $(IFLAGS) $(LFLAGS)   

chromatic_bst_bench:
	g++ ./bst_bench.cpp -o bin/chromatic_bst_bench -Isetbench/ds/brown_ext_chromatic_lf $(DFLAGS) $(CFLAGS) $(IFLAGS) $(LFLAGS)   

bronson_bst_bench:
	g++ ./bst_bench.cpp -o bin/bronson_bst_bench -Isetbench/ds/bronson_pext_bst_occ $(DFLAGS) $(CFLAGS) $(IFLAGS) $(LFLAGS) 

srivastava_abtree_pub_bench:
	g++ ./bst_bench.cpp -o bin/srivastava_abtree_pub_bench -Isetbench/ds/srivastava_abtree_pub $(DFLAGS) $(CFLAGS) $(IFLAGS) $(LFLAGS) 

srivastava_abtree_pub_bench_debug:
	g++ ./bst_bench.cpp -o bin/srivastava_abtree_pub_bench_debug -Isetbench/ds/srivastava_abtree_pub $(DFLAGS) $(CFLAGS_DEBUG) $(IFLAGS) $(LFLAGS) 



drachsler_bst_bench:
	g++ ./bst_bench.cpp -o bin/drachsler_bst_bench -Isetbench/ds/drachsler_pext_bst_lock $(DFLAGS) $(CFLAGS) $(IFLAGS) $(LFLAGS) 

natarajan_bst_bench:
	g++ ./bst_bench.cpp -o bin/natarajan_bst_bench -Isetbench/ds/natarajan_ext_bst_lf $(DFLAGS) $(CFLAGS) $(IFLAGS) $(LFLAGS) 


ellen_bst_bench_debug:
	g++ ./bst_bench.cpp -o bin/ellen_bst_bench_debug -Isetbench/ds/ellen_ext_bst_lf $(DFLAGS) $(CFLAGS_DEBUG) $(IFLAGS) $(LFLAGS) 

chromatic_bst_bench_debug:
	g++ ./bst_bench.cpp -o bin/chromatic_bst_bench_debug -Isetbench/ds/brown_ext_chromatic_lf $(DFLAGS) $(CFLAGS_DEBUG) $(IFLAGS) $(LFLAGS) 


clean:
	rm bin/*
