#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#


#---------------------build config-------------------------

DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?=

BIND_LEVELDB ?= 1
BIND_ROCKSDB ?= 1

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard db/leveldb/*.cc)
	CPPFLAGS += -DBIND_LEVELDB
endif

ifeq ($(BIND_ROCKSDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard db/rocksdb/*.cc)
	CPPFLAGS += -DBIND_ROCKSDB
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
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<

clean:
	find . -name "*.o" -delete
	$(RM) $(EXEC)

.PHONY: clean
