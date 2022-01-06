FLAGS =
LDFLAGS =
GPP = g++

### if libpapi is installed but you do not want to use it, invoke make with extra argument "has_libpapi=0"
has_libpapi=$(shell ./_check_lib.sh papi)
ifneq ($(has_libpapi), 0)
	FLAGS += -DUSE_PAPI
	LDFLAGS += -lpapi
endif

### if libnuma is installed but you do not want to use it, invoke make with extra argument "has_libnuma=0"
has_libnuma=$(shell ./_check_lib.sh numa)
ifneq ($(has_libnuma), 0)
	FLAGS += -DUSE_LIBNUMA
	LDFLAGS += -lnuma
endif

use_asan=0
ifneq ($(use_asan), 0)
	LDFLAGS += -fsanitize=address -static-libasan
endif

use_asserts=0
ifeq ($(use_asserts), 0)
	FLAGS += -DNDEBUG
endif

use_fopenmp=1
ifeq ($(use_fopenmp), 1)
	FLAGS += -fopenmp
endif

use_timelines=0
ifeq ($(use_timelines), 1)
	FLAGS += -DMEASURE_TIMELINE_STATS
endif

no_optimize=0
ifeq ($(no_optimize), 1)
	FLAGS += -O0 -fno-inline-functions -fno-inline
else
	FLAGS += -O3
endif

skip_validation=0
ifeq ($(skip_validation), 1)
	FLAGS += -DSKIP_VALIDATION
endif

# FLAGS += -march=native
FLAGS += -DMAX_THREADS_POW2=512
FLAGS += -DCPU_FREQ_GHZ=2.1 #$(shell ./experiments/get_cpu_ghz.sh)
FLAGS += -DMEMORY_STATS=if\(1\) -DMEMORY_STATS2=if\(0\)
FLAGS += -std=c++17 -mcx16 -gdwarf
FLAGS += -fno-omit-frame-pointer
# FLAGS += -DCALL_DEBUG_GC
#FLAGS += -DNO_CLEANUP_AFTER_WORKLOAD ### avoid executing data structure destructors, to save teardown time at the end of each trial (useful with massive trees)
#FLAGS += -DRAPID_RECLAMATION
# FLAGS += -DPREFILL_INSERTION_ONLY
FLAGS += -DDEBRA_ORIGINAL_FREE
#FLAGS += -DMEASURE_REBUILDING_TIME
# FLAGS += -DMEASURE_TIMELINE_STATS
FLAGS += -DUSE_TREE_STATS
#FLAGS += -DOVERRIDE_PRINT_STATS_ON_ERROR
#FLAGS += -Wno-format
FLAGS += $(xargs)

LDFLAGS += -L../lib
LDFLAGS += -I./ -I../ `find ../common -type d | sed s/^/-I/`
LDFLAGS += -lpthread
LDFLAGS += -ldl
LDFLAGS += -mrtm

.PHONY: default
default: ds-reclaim
	@echo
	@echo "===================================================================="
	@echo "== Compilation target was:"
	@echo "===================================================================="
	@echo "== 'ds-reclaim' which produces binaries for all combinations of"
	@echo "==              the make variables DATA_STRUCTURES, RECLAIMERS"
	@echo "=="
	@echo "== for example, you can specify multiple RECLAIMERS by running, eg."
	@echo "==              make RECLAIMERS='debra none' -j"
	@echo "===================================================================="
	@echo "== Other target options:"
	@echo "===================================================================="
	@echo "== 'ds-reclaim-pool' which produces binaries for combinations of"
	@echo "==                   make vars DATA_STRUCTURES, RECLAIMERS, POOLS"
	@echo "=="
	@echo "== 'all' which produces binaries for combinations of"
	@echo "== 	    make vars DATA_STRUCTURES, RECLAIMERS, POOLS, ALLOCATORS"
	@echo "=="
	@echo "==     NOTE: all target is !!DEPRECATED!!"
	@echo "==           use ds-reclaim going forward."
	@echo "===================================================================="
	@echo

bin_dir=bin
dir_guard:
	@mkdir -p $(bin_dir)


################################################################################
#### BEGIN check for proper git cloning WITH SUBMODULES
################################################################################
define submodules_err_msg=

================================================================================
==== ERROR: CANNOT FIND SUBMODULES                                          ====
================================================================================
==== You most likely used the wrong git clone command...                    ====
====                                                                        ====
==== Note that a special argument is needed for cloning:                    ====
==== git clone https://gitlab.com/trbot86/setbench.git --recurse-submodules ====
================================================================================
endef
dir_exists_tools=$(shell if [ -d "../tools" ]; then echo 1 ; else echo 0 ; fi )
dir_exists_recordmgr=$(shell if [ -d "../common/recordmgr" ]; then echo 1 ; else echo 0 ; fi )
num_files_tools=$(shell ls ../tools | wc -l 2>/dev/null )
num_files_recordmgr=$(shell ls ../common/recordmgr | wc -l 2>/dev/null )
ifeq ($(dir_exists_tools),0)
    $(error $(submodules_err_msg))
endif
ifeq ($(dir_exists_recordmgr),0)
    $(error $(submodules_err_msg))
endif
ifeq ($(num_files_tools),0)
    $(error $(submodules_err_msg))
endif
ifeq ($(num_files_recordmgr),0)
    $(error $(submodules_err_msg))
endif
################################################################################
#### END check for proper git cloning WITH SUBMODULES
################################################################################



################################################################################
#### DECIDING WHICH COMBINATIONS OF: DATA STRUCTURE, RECLAIMER, POOL, ALLOCATOR
#### SHOULD BE COMPILED
################################################################################

DATA_STRUCTURES=$(patsubst ../ds/%/adapter.h,%,$(wildcard ../ds/*/adapter.h))
RECLAIMERS=debra
POOLS=none
ALLOCATORS=new ## allocators are DEPRECATED (using LD_PRELOAD to load allocators instead!)

## statements like the below can be used to compile subsets of the data structures...
#DATA_STRUCTURES=$(patsubst ../ds/%/adapter.h,%,$(wildcard ../ds/brown_ext_ist*/adapter.h))
#DATA_STRUCTURES+=$(patsubst ../ds/%/adapter.h,%,$(wildcard ../ds/bronson*/adapter.h))

## preview the set of DATA STRUCTURES
$(info $(DATA_STRUCTURES))

################################################################################
#### CONSTRUCTING CUSTOM BUILD TARGETS FOR EACH COMBINATION
#### and adding them to combined targets:
####    ds-reclaim
####	ds-reclaim-pool
####	DEPRECATED-ds-alloc-reclaim-pool
################################################################################

## create build targets for combinations of DATA_STRUCTURES and RECLAIMERS
## and add them to a combined target: "ds-reclaim"
define create-target-ds-reclaim =
$(1).$(2): dir_guard
	$(GPP) ./main.cpp -o $(bin_dir)/$(1).$(2) -I../ds/$(1) -DDS_TYPENAME=$(1) -DRECLAIM_TYPE=$(2) $(FLAGS) $(LDFLAGS)
ds-reclaim: $(1).$(2)
endef
$(foreach ds,$(DATA_STRUCTURES), \
	$(foreach reclaim,$(RECLAIMERS), \
		$(eval $(call create-target-ds-reclaim,$(ds),$(reclaim))) \
	) \
)

# Overrides the recipe found above
wang_openbwtree.debra: dir_guard
	$(GPP) ./main.cpp ../ds/wang_openbwtree/bwtree.cpp -o $(bin_dir)/wang_openbwtree.debra -I../ds/wang_openbwtree -DDS_TYPENAME=wang_openbwtree $(FLAGS) -Wno-invalid-offsetof $(LDFLAGS) -latomic
ds-reclaim: wang_openbwtree.debra

## create build targets for combinations of DATA_STRUCTURES, RECLAIMERS and POOLS
## and add them to a combined target: "ds-reclaim-pool"
define create-target-ds-reclaim-pool =
_$(1).$(2).p$(3): dir_guard
	$(GPP) ./main.cpp -o $(bin_dir)/$(1).$(2).p$(3) -I../ds/$(1) -DDS_TYPENAME=$(1) -DRECLAIM_TYPE=$(2) -DPOOL_TYPE=$(3) $(FLAGS) $(LDFLAGS)
ds-reclaim-pool: _$(1).$(2).p$(3)
endef
$(foreach ds,$(DATA_STRUCTURES), \
	$(foreach reclaim,$(RECLAIMERS), \
		$(foreach pool,$(POOLS), \
			$(eval $(call create-target-ds-reclaim-pool,$(ds),$(reclaim),$(pool))) \
		) \
	) \
)

## THE FOLLOWING IS DEPRECATED -- USE LD_PRELOAD TO LOAD ALLOCATORS INSTEAD!
## create build targets for combinations of DATA_STRUCTURES, RECLAIMERS, POOLS and ALLOCATORS
## and add them to a combined target: "DEPRECATED-ds-alloc-reclaim-pool"
define create-target-ds-alloc-reclaim-pool =
_$(1).$(2).$(3).p$(4): dir_guard
	$(GPP) ./main.cpp -o $(bin_dir)/$(1).$(2).$(3).p$(4) -I../ds/$(1) -DDS_TYPENAME=$(1) -DALLOC_TYPE=$(2) -DRECLAIM_TYPE=$(3) -DPOOL_TYPE=$(4) $(FLAGS) $(LDFLAGS)
all: _$(1).$(2).$(3).p$(4)
endef

$(foreach ds,$(DATA_STRUCTURES), \
	$(foreach alloc,$(ALLOCATORS), \
		$(foreach reclaim,$(RECLAIMERS), \
			$(foreach pool,$(POOLS), \
				$(eval $(call create-target-ds-alloc-reclaim-pool,$(ds),$(alloc),$(reclaim),$(pool))) \
			) \
		) \
	) \
)

clean:
	rm $(bin_dir)/*.out
