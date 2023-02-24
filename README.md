# YCSB-cpp

Yahoo! Cloud Serving Benchmark([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C) with some additions

 * Tail latency report using [HdrHistogram_c](https://github.com/HdrHistogram/HdrHistogram_c)
 * Small changes to make it behave more like the original YCSB
 * Supported Databases: LevelDB, RocksDB, LMDB, WiredTiger

## NOTICE

This repo is forked from [ls4154/YCSB-cpp](https://github.com/ls4154/YCSB-cpp) to support WiredTiger.

# Build WiredTiger

Clone the source:
```
git clone git://github.com/wiredtiger/wiredtiger.git
mkdir -p wiredtiger/build
cd wiredtiger/build
```

Enable built-in compressors and build
```
cmake -DHAVE_BUILTIN_EXTENSION_LZ4=1 -DHAVE_BUILTIN_EXTENSION_SNAPPY=1 -DHAVE_BUILTIN_EXTENSION_ZLIB=1 -DHAVE_BUILTIN_EXTENSION_ZSTD=1 -DENABLE_STATIC=1 ../.
make -j$(nproc)
sudo make install
```

## Build YCSB-cpp

## Build with Makefile on GNU

Simply use `make` to build.

```
git clone https://github.com/kabu1204/YCSB-cpp
cd YCSB-cpp
git submodule update --init
make
```

Databases to bind must be specified as build options. You may also need to add additional link flags (e.g., `-lsnappy`).

To bind LevelDB:
```
make BIND_LEVELDB=1
```

To build with additional libraries and include directories:
```
make BIND_LEVELDB=1 EXTRA_CXXFLAGS=-I/example/leveldb/include \
                    EXTRA_LDFLAGS="-L/example/leveldb/build -lsnappy"
```

Or modify config section in `Makefile`.

RocksDB build example:
```
EXTRA_CXXFLAGS ?= -I/example/rocksdb/include
EXTRA_LDFLAGS ?= -L/example/rocksdb -ldl -lz -lsnappy -lzstd -lbz2 -llz4

BIND_ROCKSDB ?= 1
```

## Build with CMake+vcpkg on Windows
### Prerequisite

1. You need to install Visual Studio Community first, for example VS2019.
2. [vcpkg](https://github.com/microsoft/vcpkg)
3. [CMake](https://cmake.org/download/)

### Building

The following steps are done in *Developer Powershell for VS 2019*(StartMenu - Visual Studio 2019 - Developer Powershell for VS 2019)

1. Install RocksDB
    ```powershell
   cd <vcpkg_root>
   .\vcpkg.exe install rocksdb[*]:x64-windows
    ```
    This will automatically install dependent compression libraries(lz4, snappy, zstd, zlib), and build rocksdb with these libraries enabled.

    `<vcpkg_root>` is the path where you've installed the vcpkg, `C:/src/vcpkg` for example.

2. Clone YCSB-cpp
    ```powershell
   cd <somewhere>
   git clone https://github.com/kabu1204/YCSB-cpp
   cd YCSB-cpp
   git submodule update --init
   ```
3. Build YCSB-cpp
    ```powershell
    mkdir build
    cd build
    cmake -DBIND_ROCKSDB=1 -DWITH_SNAPPY=1 -DCMAKE_TOOLCHAIN_FILE=<vcpkg_root>/scripts/buildsystems/vcpkg.cmake ..
    msbuild ycsb-cpp.sln /p:Configuration=Release
    ```
    `-DCMAKE_TOOLCHAIN_FILE=<vcpkg_root>/scripts/buildsystems/vcpkg.cmake` enables CMake `find_packege()` to find libraries 
    installed by vcpkg. You do not need to use backslash '\' in your path here.
    
    `-DBIND_ROCKSDB=1` binds rocksdb.
    
    `-DWITH_SNAPPY=1` tells CMake to link snappy with YCSB, same for `WITH_LZ4`, `WITH_ZLIB` and `WITH_ZSTD`.

    The executable `ycsb.exe` can be found in `build/Release`.

## Running

Run workload A on wiredtiger:
```
./ycsb -load -run -db wiredtiger -P workloads/workloada -P wiredtiger/wiredtiger.properties -s
```

Run workload A on wiredtiger, with len(key)=8B and len(value)=24B:
```
./ycsb -load -run -db wiredtiger -P workloads/workloada -P wiredtiger/wiredtiger.properties -s \
-p fixedkey8b=true -p fixedfieldlen=true -p fieldcount=1 -p fieldlength=24
```

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
