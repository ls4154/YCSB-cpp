# YCSB-cpp

Yahoo! Cloud Serving Benchmark([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C) with following changes.

 * Make Zipf distribution and data value more similar to the original YCSB
 * Status and latency report during benchmark
 * Supported Databases: LevelDB, RocksDB, LMDB

## Building

Simply use `make` to build.

To bind only LevelDB:
```
make BIND_LEVELDB=1 BIND_ROCKSDB=0 BIND_LMDB=0
```

To build with additional libraries and include directories:
```
make EXTRA_CXXFLAGS=-I/example/leveldb/include EXTRA_LDFLAGS=-L/example/leveldb/build
```

or modify config section in `Makefile`.

## Running

Load data with leveldb:
```
./ycsb -load -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```

Run workload A with leveldb:
```
./ycsb -run -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```

Load and run workload B with rocksdb:
```
./ycsb -load -run -db rocksdb -P workloads/workloadb -P rocksdb/rocksdb.properties -s
```

Pass additional properties:
```
./ycsb -load -db leveldb -P workloads/workloadb -P rocksdb/rocksdb.properties \
    -p threadcount=4 -p recordcount=10000000 -p leveldb.cache_size=134217728 -s
```
