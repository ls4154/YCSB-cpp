cmake_minimum_required(VERSION 3.10)
project(ycsb-cpp LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)

set(CMAKE_BUILD_TYPE Release)
set(CompilerFlags
        CMAKE_CXX_FLAGS
        CMAKE_CXX_FLAGS_DEBUG
        CMAKE_CXX_FLAGS_RELEASE
        CMAKE_C_FLAGS
        CMAKE_C_FLAGS_DEBUG
        CMAKE_C_FLAGS_RELEASE
        )

if(MSVC)
    foreach(CompilerFlag ${CompilerFlags})
        string(REPLACE "/MD" "/MT" ${CompilerFlag} "${${CompilerFlag}}")
    endforeach()
endif()

option(BIND_ROCKSDB "build with rocksdb" OFF)
option(BIND_WIREDTIGER "build with wiredtiger" OFF)
option(WITH_ZLIB "linking YCSB with zlib; needed by HdrHISTOGRAM, DO NOT TURN OFF" ON)
option(WITH_LZ4 "linking YCSB with lz4" OFF)
option(WITH_SNAPPY "linking YCSB with snappy" OFF)
option(WITH_ZSTD "linking YCSB with zstd" OFF)

file(GLOB_RECURSE YCSB_CORE_SRC "core/*.cc")

add_executable(ycsb ${YCSB_CORE_SRC})
target_include_directories(ycsb PRIVATE ${PROJECT_SOURCE_DIR})

if (BIND_ROCKSDB)
    message(STATUS "BIND_ROCKSDB - ON")
    set(WITH_ZLIB ON)  # rocksdb needs zlib
    file(GLOB_RECURSE YCSB_ROCKSDB_SRC "rocksdb/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_ROCKSDB_SRC})
    find_package(RocksDB CONFIG REQUIRED)
    target_link_libraries(ycsb PRIVATE RocksDB::rocksdb rpcrt4.lib)
else()
    message(STATUS "BIND_ROCKSDB - OFF")
endif()

if (BIND_WIREDTIGER)
    message(STATUS "BIND_WIREDTIGER - ON")
    file(GLOB_RECURSE YCSB_WT_SRC "wiredtiger/*.cc")
    target_sources(ycsb PRIVATE ${YCSB_WT_SRC})
    find_library(WIREDTIGER_LIB wiredtiger REQUIRED)
    find_path(WIREDTIGER_INCLUDE_DIR "wiredtiger.h" REQUIRED)
    target_link_libraries(ycsb PRIVATE ${WIREDTIGER_LIB})
    target_include_directories(ycsb PRIVATE ${WIREDTIGER_INCLUDE_DIR})
else()
    message(STATUS "BIND_WIREDTIGER - OFF")
endif ()

if(WITH_ZLIB)
    message(STATUS "WITH_ZLIB - ON")
    find_package(ZLIB REQUIRED)
    target_link_libraries(ycsb PRIVATE ZLIB::ZLIB)
else()
    message(STATUS "WITH_ZLIB - OFF")
endif()

if(WITH_SNAPPY)
    message(STATUS "WITH_SNAPPY - ON")
    find_package(Snappy CONFIG REQUIRED)
    target_link_libraries(ycsb PRIVATE Snappy::snappy)
else()
    message(STATUS "WITH_SNAPPY - OFF")
endif()

if(WITH_LZ4)
    message(STATUS "WITH_LZ4 - ON")
    find_package(lz4 CONFIG REQUIRED)
    target_link_libraries(main PRIVATE lz4::lz4)
else()
    message(STATUS "WITH_LZ4 - OFF")
endif()

if(WITH_ZSTD)
    message(STATUS "WITH_ZSTD - ON")
    find_package(zstd CONFIG REQUIRED)
    target_link_libraries(main PRIVATE $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>)
else()
    message(STATUS "WITH_ZSTD - OFF")
endif()

add_subdirectory(HdrHistogram_c)
include_directories(HdrHistogram_c/include)
add_compile_definitions(HDRMEASUREMENT)
add_dependencies(ycsb hdr_histogram_static)
target_link_libraries(ycsb PRIVATE hdr_histogram_static)