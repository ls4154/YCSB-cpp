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

# Extra options
DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?= -ldl -lz -lsnappy -lzstd -lbz2 -llz4

# HdrHistogram for tail latency report
BIND_HDRHISTOGRAM ?= 1
# Build and statically link library, submodule required
BUILD_HDRHISTOGRAM ?= 1

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O3
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

ifeq ($(BIND_REDIS), 1)
	LDFLAGS += -lredis++ -lhiredis
	SOURCES += $(wildcard redis/*.cc)
endif

ifeq ($(BIND_ROCKSDBCLI), 1)
	CXXFLAGS += -Wno-address-of-packed-member -DERPC_INFINIBAND=true
	LDFLAGS += -lerpc -libverbs -lnuma
	SOURCES += rocksdb-clisvr/rocksdb_cli.cc
	SVR = rocksdb_svr
	SVR_SOURCE = rocksdb-clisvr/rocksdb_svr.cc
	SVRFLAGS = -lrocksdb
endif

ifeq ($(BIND_POSTGRES), 1)
	LDFLAGS += -lpq
	SOURCES += $(wildcard postgres/*.cc)
endif

ifeq ($(BIND_SQLITE), 1)
	CXXFLAGS += -Wno-address-of-packed-member -DERPC_INFINIBAND=true
	LDFLAGS += -lerpc -libverbs -lnuma
	SOURCES += sqlite/sqlite_cli.cc
	SVR = sqlite_svr
	SVR_SOURCE = sqlite/sqlite_svr.cc ../sqlite/build/sqlite3.o
	SVRFLAGS = 
endif

ifeq ($(BIND_SQLOCAL), 1)
	SOURCES += $(wildcard sqlocal/*.cc)
	OBJECTS += ../sqlite/build/sqlite3.o
endif

CXXFLAGS += -std=c++17 -pthread $(EXTRA_CXXFLAGS) -I./ -I$(YAMLCPP_DIR)/include
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o) $(YAMLCPP_LIB)
DEPS += $(SOURCES:.cc=.d)
EXEC = ycsb

HDRHISTOGRAM_DIR = HdrHistogram_c
HDRHISTOGRAM_LIB = $(HDRHISTOGRAM_DIR)/build/src/libhdr_histogram_static.a
YAMLCPP_DIR = yaml-cpp
YAMLCPP_LIB = $(YAMLCPP_DIR)/build/libyaml-cpp.a

ifeq ($(BIND_HDRHISTOGRAM), 1)
ifeq ($(BUILD_HDRHISTOGRAM), 1)
	CXXFLAGS += -I$(HDRHISTOGRAM_DIR)/include
	OBJECTS += $(HDRHISTOGRAM_LIB)
else
	LDFLAGS += -lhdr_histogram
endif
CPPFLAGS += -DHDRMEASUREMENT
endif

all: $(EXEC) $(SVR)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

$(SVR): $(SVR_SOURCE)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) $(SVRFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d: %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

$(HDRHISTOGRAM_DIR)/CMakeLists.txt:
	@echo "Download HdrHistogram_c"
	@git submodule update --init

$(HDRHISTOGRAM_DIR)/build/Makefile: $(HDRHISTOGRAM_DIR)/CMakeLists.txt
	@cmake -DCMAKE_BUILD_TYPE=Release -S $(HDRHISTOGRAM_DIR) -B $(HDRHISTOGRAM_DIR)/build


$(HDRHISTOGRAM_LIB): $(HDRHISTOGRAM_DIR)/build/Makefile
	@echo "Build HdrHistogram_c"
	@make -C $(HDRHISTOGRAM_DIR)/build -j

$(YAMLCPP_DIR)/CMakeLists.txt:
	@echo "Download yaml-cpp"
	@git submodule update --init

$(YAMLCPP_DIR)/build/Makefile: $(YAMLCPP_DIR)/CMakeLists.txt
	@cmake -DCMAKE_BUILD_TYPE=Release -S $(YAMLCPP_DIR) -B $(YAMLCPP_DIR)/build

$(YAMLCPP_LIB): $(YAMLCPP_DIR)/build/Makefile
	@echo "Build yaml-cpp"
	@make -C $(YAMLCPP_DIR)/build -j

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC) $(SVR)

.PHONY: clean
