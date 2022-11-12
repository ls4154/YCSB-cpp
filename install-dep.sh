#!/bin/bash

dir=$1

sudo apt-get install -y \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    fio \
    nload \
    iftop

git clone https://github.com/facebook/rocksdb.git $dir/rocksdb
cd $dir/rocksdb
git checkout v7.7.2
make shared_lib -j

git clone https://github.com/dassl-uiuc/YCSB-cpp.git $dir/YCSB-cpp
cd $dir/YCSB-cpp


make BIND_ROCKSDB=1 EXTRA_CXXFLAGS="-I$dir/rocksdb/include -L$dir/rocksdb" -j
