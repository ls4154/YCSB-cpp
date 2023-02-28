# Prerequisite

1. You need to install Visual Studio Community first, for example VS2019.
2. [vcpkg](https://github.com/microsoft/vcpkg)
3. [CMake](https://cmake.org/download/)

# Building

The following steps are done in *Developer Powershell for VS 2019*(StartMenu - Visual Studio 2019 - Developer Powershell for VS 2019)

1. Install RocksDB/leveldb/lmdb
    ```powershell
   cd <vcpkg_root>
   .\vcpkg.exe install rocksdb[*]:x64-windows-static
   .\vcpkg.exe install leveldb[snappy]:x64-windows-static
   .\vcpkg.exe install lmdb:x64-windows-static
    ```
   This will automatically install dependent compression libraries(lz4, snappy, zstd, zlib), and build rocksdb with these libraries enabled.

   `<vcpkg_root>` is the path where you've installed the vcpkg, `C:/src/vcpkg` for example.

2. Install WiredTiger

    see [wiredtiger/README.md](wiredtiger/README.md)

3. Clone YCSB-cpp
    ```powershell
   cd <somewhere>
   git clone https://github.com/ls4154/YCSB-cpp.git
   cd YCSB-cpp
   git submodule update --init
   ```
4. Build YCSB-cpp
    ```powershell
    mkdir build
    cd build
    cmake -DBIND_ROCKSDB=1 -DBIND_WIREDTIGER=1 -DWITH_SNAPPY=1 -DCMAKE_TOOLCHAIN_FILE=<vcpkg_root>/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-windows-static ..
    msbuild ycsb-cpp.sln /p:Configuration=Release
    ```
    `-DCMAKE_TOOLCHAIN_FILE=<vcpkg_root>/scripts/buildsystems/vcpkg.cmake` enables CMake `find_packege()` to find libraries
    installed by vcpkg. You do not need to use backslash '\' in your path here.

    `-DBIND_ROCKSDB=1` binds rocksdb.

    `-DBIND_LEVELDB=1` binds leveldb.

    `-DBIND_LMDB=1` binds lmdb.

    `-DBIND_WIREDTIGER=1` binds wiredtiger.

    `-DWITH_SNAPPY=1` tells CMake to link snappy with YCSB, same for `WITH_LZ4`, `WITH_ZLIB` and `WITH_ZSTD`.

   The executable `ycsb.exe` can be found in `build/Release`.