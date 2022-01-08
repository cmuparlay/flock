CPP = g++
INCLUDES = -I../include -I../include/flock -I../structures
CFLAGS = -std=c++17 -O3 -march=native -DHOMEGROWN -pthread -DNDEBUG $(INCLUDES)
CFLAGS_DEBUG = -std=c++17 -g -march=native -DHOMEGROWN -pthread -DNDEBUG
LFLAGS = -L/usr/local/lib -ljemalloc
#LFLAGS = -ljemalloc
LOCK_DEPS = lock.h log.h defs.h ptr_type.h lock_type.h
MEM_DEPS = epoch.h tagged_pool.h
DEPS = $(../include/locks/:LOCK_DEPS) $(../include/memory/:MEM_DEPS) zipfian.h parse_command_line.h

LISTS = list dlist 
TREES = leaftree blockleaftree arttree btree 
HASH = hash
OTHER = harris_list harris_list_opt
DS = $(LISTS) $(TREES) $(HASH)
ALL = $(DS) $(OTHER) blockleaftree_bl

SDIR = ../structures
HDIR = ../ds

all : $(ALL)

$(DS) : $(SDIR)/$(@:=.h) $(DEPS)
	$(CPP) $(CFLAGS) -include $(SDIR)/$(@:=.h) -o $@ test_sets.cpp $(LFLAGS)

blockleaftree_bl : $(SDIR)/blockleaftree.h $(DEPS)
	$(CPP) $(CFLAGS) -DBALANCED -include $(SDIR)/blockleaftree.h -o $@ test_sets.cpp $(LFLAGS)

harris_list : $(HDIR)/harris_list/harris_ll_adapter.h $(HDIR)/harris_list/HarrisLinkedList.hpp $(DEPS)
	$(CPP) $(CFLAGS) -include $(HDIR)/harris_list/harris_ll_adapter.h -o $@ test_sets.cpp $(LFLAGS)

harris_list_opt : $(HDIR)/harris_list_opt/harris_ll_adapter.h $(HDIR)/harris_list_opt/HarrisLinkedList.hpp $(DEPS)
	$(CPP) $(CFLAGS) -include $(HDIR)/harris_list_opt/harris_ll_adapter.h -o $@ test_sets.cpp $(LFLAGS)

clean :
	rm -f $(ALL)
