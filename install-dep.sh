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
    cmake \
    cgroup-tools

git clone https://github.com/sewenew/redis-plus-plus.git $dir/redis++
cd $dir/redis++
mkdir build
cd build
cmake ..
make -j
sudo make install
cd ..
