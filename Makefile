#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#


#---------------------build config-------------------------

DEBUG_BUILD = 0
EXTRA_CXXFLAGS =
EXTRA_LDFLAGS =

LEVELDB_BINDING = 1

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2 -DNDEBUG
endif

ifeq ($(LEVELDB_BINDING), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard db/leveldb/*.cc)
endif

CXXFLAGS += -std=c++11 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
EXEC = ycsbc

all: $(EXEC)

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

.cc.o:
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	find . -name "*.o" -delete
	$(RM) $(EXEC)

.PHONY: clean
