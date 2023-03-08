#!/bin/bash

dir=$1

sudo apt-get install -y \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    libhiredis-dev \
    fio \
    nload \
    iftop \
    cmake

git clone https://github.com/facebook/rocksdb.git $dir/rocksdb
cd $dir/rocksdb
git checkout v7.7.2
make shared_lib -j
cd ..

git clone https://github.com/sewenew/redis-plus-plus.git $dir/redis++
cd $dir/redis++
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ..

git clone https://github.com/dassl-uiuc/YCSB-cpp.git $dir/YCSB-cpp
cd $dir/YCSB-cpp


make BIND_ROCKSDB=1 EXTRA_CXXFLAGS="-I$dir/rocksdb/include -L$dir/rocksdb" -j

echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/$dir/rocksdb/" >> ~/.bashrc
source ~/.bashrc
