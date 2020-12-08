# YCSB-cpp

Yahoo! Cloud Serving Benchmark([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C).

Currently LevelDB and RocksDB are supported.

## Building

Simply use `make` to build.

To bind only LevelDB:
```
make BIND_LEVELDB=1 BIND_ROCKSDB=0
```

To build with additional libraries and include directories:
```
make EXTRA_CXXFLAGS=-I/example/leveldb/include EXTRA_LDFLAGS=-L/example/leveldb/build
```

## Running

Run YCSB with leveldb:
```
./ycsb -load -run -db leveldb -P workloads/workloada -P leveldb/leveldb.properties -s
```
