#!/bin/bash

set -x

source $(dirname "$0")/config.sh

if [ $# -ge 1 ]; then
    mode=$1
else
    mode=run
fi

function build_rocksdb() {
    ssh -o StrictHostKeyChecking=no $user@$server "cd $dir; \
            rm ycsb rocksdb_svr; \
            make BIND_ROCKSDBCLI=1 EXTRA_CXXFLAGS=\"-I/data/eRPC/src -I/data/eRPC/third_party/asio/include -I/data/rocksdb/include -L/data/eRPC/build -L/data/rocksdb\" -j"
}

function build_redis() {
    ssh -o StrictHostKeyChecking=no $user@$server "cd $dir; rm ycsb; make BIND_REDIS=1 -j"
}

function build_sqlite() {
    ssh -o StrictHostKeyChecking=no $user@$server "cd $dir; \
            rm ycsb sqlite_svr; \
            make BIND_SQLITE=1 EXTRA_CXXFLAGS=\"-I/data/eRPC/src -I/data/eRPC/third_party/asio/include -I/data/sqlite/build -L/data/eRPC/build\" -j"
}

function run_one() {
    db=$1
    mode=$2

    build_$db

    $dir/scripts/run_$db.sh cephfs $mode
    $dir/scripts/run_$db.sh sync $mode
    $dir/scripts/run_$db.sh sync_ncl $mode
}

function run_all() {
    mode=$1

    run_one rocksdb $mode
    run_one redis $mode
    run_one sqlite $mode

    if [ $mode = load ]; then
        mkdir -p /data/result/fig
        python3 $dir/scripts/draw/insert_only.py /data/result
    else
        mkdir -p /data/result/fig
        python3 $dir/scripts/draw/ycsb.py /data/result
    fi
}

run_all $mode
