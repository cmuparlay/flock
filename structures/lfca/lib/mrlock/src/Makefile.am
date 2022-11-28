AUTOMAKE_OPTIONS = subdir-objects

EXTRA_DIST = timehelper.h \
			 bitset.h \
			 threadbarrier.h \
			 mrlock.h \
			 strategy/lockablebase.h \
			 strategy/mrlockable.h

bin_PROGRAMS = locktester

locktester_SOURCES = main.cc \
					 timehelper.cc \
				 	 strategy/lockablebase.cc \
				 	 strategy/mrlockable.cc

locktester_CPPFLAGS = $(WARN_FLAGS) \
					  -I$(top_builddir)/src -I$(top_srcdir)/src \
					  -std=c++11

locktester_LDADD = $(BOOST_LDFLAGS) -lboost_random
