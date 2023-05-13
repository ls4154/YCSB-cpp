#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
#


#---------------------build config-------------------------

# Database bindings
BIND_WIREDTIGER ?= 0
BIND_LEVELDB ?= 0
BIND_ROCKSDB ?= 0
BIND_LMDB ?= 0
BIND_SQLITE ?= 0

# Extra options
DEBUG_BUILD ?=
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?=

# HdrHistogram for tail latency report
BIND_HDRHISTOGRAM ?= 1
# Build and statically link library, submodule required
BUILD_HDRHISTOGRAM ?= 1

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_WIREDTIGER), 1)
	LDFLAGS += -lwiredtiger
	SOURCES += $(wildcard wiredtiger/*.cc)
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard leveldb/*.cc)
endif

ifeq ($(BIND_ROCKSDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard rocksdb/*.cc)
endif

ifeq ($(BIND_LMDB), 1)
	LDFLAGS += -llmdb
	SOURCES += $(wildcard lmdb/*.cc)
endif

ifeq ($(BIND_SQLITE), 1)
	LDFLAGS += -lsqlite3
	SOURCES += $(wildcard sqlite/*.cc)
endif

CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
DEPS += $(SOURCES:.cc=.d)
EXEC = ycsb

HDRHISTOGRAM_DIR = HdrHistogram_c
HDRHISTOGRAM_LIB = $(HDRHISTOGRAM_DIR)/src/libhdr_histogram_static.a

ifeq ($(BIND_HDRHISTOGRAM), 1)
ifeq ($(BUILD_HDRHISTOGRAM), 1)
	CXXFLAGS += -I$(HDRHISTOGRAM_DIR)/include
	OBJECTS += $(HDRHISTOGRAM_LIB)
else
	LDFLAGS += -lhdr_histogram
endif
CPPFLAGS += -DHDRMEASUREMENT
endif

all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d: %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

$(HDRHISTOGRAM_DIR)/CMakeLists.txt:
	@echo "Download HdrHistogram_c"
	@git submodule update --init

$(HDRHISTOGRAM_DIR)/Makefile: $(HDRHISTOGRAM_DIR)/CMakeLists.txt
	@cmake -DCMAKE_BUILD_TYPE=Release -S $(HDRHISTOGRAM_DIR) -B $(HDRHISTOGRAM_DIR)


$(HDRHISTOGRAM_LIB): $(HDRHISTOGRAM_DIR)/Makefile
	@echo "Build HdrHistogram_c"
	@make -C $(HDRHISTOGRAM_DIR)

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

.PHONY: clean
