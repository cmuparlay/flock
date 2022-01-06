##################################
## DO NOT USE THIS DIRECTLY.
## EXECUTE ./compile.sh INSTEAD!
##################################

CFLAGS =
LDFLAGS =
CC=g++

#LDFLAGS += -fsanitize=address -static-libasan
CFLAGS += -DMEMORY_STATS=if\(1\) -DMEMORY_STATS2=if\(0\)
CFLAGS += -DMAX_THREADS_POW2=256
CFLAGS += -DCPU_FREQ_GHZ=2.1 #$(shell ../microbench/experiments/get_cpu_ghz.sh)
CFLAGS += -g -std=c++17 -O3 -lstdc++fs
CFLAGS += -fopenmp
CFLAGS += $(xargs)
CFLAGS += -Wno-format
CFLAGS += -DNDEBUG
CFLAGS += -DNO_CLEANUP_AFTER_WORKLOAD

### if you do not have PAPI, comment out these two lines
#CFLAGS += -DUSE_PAPI
#LDFLAGS += -lpapi
#
#LDFLAGS += -lnuma

workload=TPCC
data_structure_name=brown_ext_abtree_lf
data_structure_opts=
data_structure_path=../ds/$(data_structure_name)
extra_src_dirs=

.SUFFIXES: .o .cpp .h
bindir=bin
odir=$(bindir)/OBJS_$(workload)_$(data_structure_name)/macrobench
SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./storage/index/ ./system/
#SRC_DIRS += ../common/rlu/
SRC_DIRS += $(extra_src_dirs)
CFLAGS += -I../ $(patsubst %,-I%,$(SRC_DIRS)) `find ../common -type d | sed s/^/-I/`

#CFLAGS += -DSKIP_PERMUTATIONS
CFLAGS += -DNOGRAPHITE=1 -DWORKLOAD=$(workload)
#CFLAGS += -DHASH_PRIMARY_KEYS
CFLAGS += -DALIGNED_ALLOCATIONS
CFLAGS += -fno-omit-frame-pointer
CFLAGS += -I$(data_structure_path) $(data_structure_opts)
LDFLAGS += $(CFLAGS)
LDFLAGS += -L. -L../lib -g
LDFLAGS += -lpthread
#LDFLAGS += -lrt
#LDFLAGS += -ldl

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(foreach obj, $(CPPS:.cpp=.o), $(odir)/$(obj))
dir_guard=@mkdir -p $(@D)

all: $(bindir)/rundb_$(workload)_$(data_structure_name)

$(bindir)/rundb_$(workload)_$(data_structure_name): $(OBJS)
	$(dir_guard)
	$(CC) -o $@ $^ $(LDFLAGS)

$(odir)/%.o: %.cpp
	$(dir_guard)
	$(CC) -c $(CFLAGS) -o $@ $<

clean:
	@rm -f $(bindir)/rundb_$(workload)_$(data_structure_name) 2>&1 >/dev/null
	@rm -r -f $(odir) 2>&1 >/dev/null

clean_objs:
	@rm -r -f $(odir) 2>&1 >/dev/null
