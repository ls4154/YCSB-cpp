#!/bin/bash

set -x

res_dir=/data/result/recover
rocksdb_dir=/data/rocksdb  # rocksdb library directory

source $(dirname "$0")/config.sh

mkdir -p $res_dir

function build_rocksdb() {
    ssh -o StrictHostKeyChecking=no $user@$server "cd $dir; \
            rm -f ycsb; \
            make BIND_ROCKSDB=1 EXTRA_CXXFLAGS=\"-I/data/rocksdb/include -L/data/rocksdb\" -j"
}

function build_redis() {
    ssh -o StrictHostKeyChecking=no $user@$server "cd $dir; rm -f ycsb; make BIND_REDIS=1 -j"
}

function build_sqlite() {
    ssh -o StrictHostKeyChecking=no $user@$server "cd $dir; \
            rm -f ycsb; \
            make BIND_SQLOCAL=1 EXTRA_CXXFLAGS=\"-I/data/sqlite/build\" -j"
}

function run_zk() {
    stop_zk || true
    rm -rf $zkdir/zookeeper
    $zkdir/bin/zkServer.sh start
}

function stop_zk() {
    $zkdir/bin/zkServer.sh stop
}

function run_ncl_server() {
    kill_ncl_server || true
    for i in ${!replica[@]}
    do
        r=${replica[$i]}
        ssh -o StrictHostKeyChecking=no $user@$r "nohup $ncl_dir/server > $ncl_dir/server$i.log 2>&1 &"
    done
}

function kill_ncl_server() {
    for r in ${replica[@]}
    do
        ssh -o StrictHostKeyChecking=no $user@$r "pid=\$(sudo lsof -i :8011 | awk 'NR==2 {print \$2}') ; \
            sudo kill -2 \$pid 2> /dev/null || true "
        sleep 1
        ssh -o StrictHostKeyChecking=no $user@$r "ps aux | grep $ncl_dir/server"
    done
}

function run_rocksdb() {
    backend=$1

    if [ $backend = cephfs ]; then
        db_dir="/mnt/cephfs/ycsb-rocksdb"
        extra_lib=""
    elif [ $backend = ncl ]; then
        db_dir="/mnt/cephfs/ycsb-rocksdb"
        extra_lib="LD_PRELOAD=$ncl_dir/libcsl.so"
    else
        db_dir="/data/ycsb-rocksdb"
        extra_lib=""
    fi

    mkdir -p $db_dir
    sudo rm -rf $db_dir/*

    build_rocksdb

    mkdir -p $res_dir/rocksdb
    csv=$res_dir/rocksdb/$backend.csv
    output=$dir/output.txt
    rm $csv

    ssh -o StrictHostKeyChecking=no $user@localhost "$extra_lib LD_LIBRARY_PATH=$rocksdb_dir $dir/ycsb -load -db rocksdb \
        -P $dir/workloads/workloada \
        -P $dir/rocksdb/rocksdb.properties \
        -s -threads 1 \
        -p rocksdb.destroy=true \
        -p rocksdb.dbname=$db_dir \
        -p recordcount=400000"
    
    ssh -o StrictHostKeyChecking=no $user@localhost "$extra_lib LD_LIBRARY_PATH=$rocksdb_dir $dir/ycsb -load -db rocksdb \
        -P $dir/workloads/workloada \
        -P $dir/rocksdb/rocksdb.properties \
        -s -threads 1 \
        -p rocksdb.destroy=false \
        -p rocksdb.dbname=$db_dir \
        -p recordcount=0 | tee $output"

    if [ $backend = ncl ]; then
        echo "sync peers",`cat $output | grep "sync peers" | awk 'NR==1 {print $3}'` >> $csv
        echo "get peer",`cat $output | grep "get peer" | awk 'NR==1 {print $3}'` >> $csv
        echo "connect",`cat $output | grep "connect" | awk 'NR==1 {print $2}'` >> $csv
        echo "recover",`cat $output | grep "recover" | awk 'NR==1 {print $2}'` >> $csv
    fi
    echo "after open",`cat $output | grep "after open" | awk 'NR==1 {print $3}'` >> $csv
    echo "end recover",`cat $output | grep "end recovery" | awk 'NR==1 {print $4}'` >> $csv
}

function kill_redis() {
    signal=$1
    pid=`sudo lsof -i :6379 | awk 'NR==2 {print $2}'`
    sudo kill -$signal $pid 2> /dev/null || true
    ps aux | grep /data/redis/src/redis-server
}

function run_redis() {
    backend=$1

    if [ $backend = cephfs ]; then
        db_dir="/mnt/cephfs/ycsb-redis"
        extra_lib=""
        conf="$dir/redis/redis.conf"
    elif [ $backend = ncl ]; then
        db_dir="/mnt/cephfs/ycsb-redis"
        extra_lib="LD_PRELOAD=$ncl_dir/libcsl.so"
        conf="$dir/redis/redis.conf"
    else
        db_dir="/data/ycsb-redis"
        extra_lib=""
        conf="$dir/redis/redis_local.conf"
    fi

    build_redis

    mkdir -p $res_dir/redis
    csv=$res_dir/redis/$backend.csv
    output=$dir/output.txt
    rm $csv

    mkdir -p $db_dir
    rm -rf $db_dir/*
    kill_redis 9
    ssh -o StrictHostKeyChecking=no $user@localhost "$extra_lib nohup /data/redis/src/redis-server $conf" &

    sleep 2

    $dir/ycsb -load -db redis \
        -P $dir/workloads/workloada \
        -P $dir/redis/redis.properties \
        -s -threads 1 \
        -p recordcount=400000
    
    kill_redis 2

    sleep 5

    ssh -o StrictHostKeyChecking=no $user@localhost "$extra_lib nohup /data/redis/src/redis-server $conf | tee $output" &

    sleep 5

    kill_redis 2

    if [ $backend = ncl ]; then
        echo "sync peers",`cat $output | grep "sync peers" | awk 'NR==1 {print $3}'` >> $csv
        echo "get peer",`cat $output | grep "get peer" | awk 'NR==1 {print $3}'` >> $csv
        echo "connect",`cat $output | grep "connect" | awk 'NR==1 {print $2}'` >> $csv
        echo "recover",`cat $output | grep "recover" | awk 'NR==1 {print $2}'` >> $csv
    fi
    echo "after open",`cat $output | grep "after open" | awk 'NR==1 {print $3}'` >> $csv
    echo "end recover",`cat $output | grep "DB loaded from incr file" | awk 'NR==1 {print $(NF-1)}'` >> $csv
}

run_sqlite() {
    backend=$1

    if [ $backend = cephfs ]; then
        db_dir="/mnt/cephfs/ycsb-sqlite"
        extra_lib=""
    elif [ $backend = ncl ]; then
        db_dir="/mnt/cephfs/ycsb-sqlite"
        extra_lib="LD_PRELOAD=$ncl_dir/libcsl.so"
    else
        db_dir="/data/ycsb-sqlite"
        extra_lib=""
    fi

    mkdir -p $db_dir
    sudo rm -rf $db_dir/*

    build_sqlite

    mkdir -p $res_dir/sqlite
    csv=$res_dir/sqlite/$backend.csv
    output=$dir/output.txt
    rm $csv

    ssh -o StrictHostKeyChecking=no $user@localhost "$extra_lib $dir/ycsb -load -db sqlocal \
        -P $dir/workloads/workloada \
        -P $dir/sqlocal/sqlocal.properties \
        -s -threads 1 \
        -p sqlocal.dbname=$db_dir/ycsb.db \
        -p recordcount=6600"
    
    ssh -o StrictHostKeyChecking=no $user@localhost "$extra_lib $dir/ycsb -load -db sqlocal \
        -P $dir/workloads/workloada \
        -P $dir/sqlocal/sqlocal.properties \
        -s -threads 1 \
        -p sqlocal.dbname=$db_dir/ycsb.db \
        -p recordcount=0 | tee $output"

    if [ $backend = ncl ]; then
        echo "sync peers",`cat $output | grep "sync peers" | awk 'NR==1 {print $3}'` >> $csv
        echo "get peer",`cat $output | grep "get peer" | awk 'NR==1 {print $3}'` >> $csv
        echo "connect",`cat $output | grep "connect" | awk 'NR==1 {print $2}'` >> $csv
        echo "recover",`cat $output | grep "recover" | awk 'NR==1 {print $2}'` >> $csv
    fi
    echo "after open",`cat $output | grep "after open" | awk 'NR==1 {print $3}'` >> $csv
    echo "end recover",`cat $output | grep "wal load duration" | awk 'NR==1 {print $4}'` >> $csv
}

function run_ncl() {
    db=$1

    run_zk

    run_ncl_server

    run_$db ncl

    kill_ncl_server

    stop_zk
}

run_ncl rocksdb
run_ncl redis
run_ncl sqlite

run_rocksdb cephfs
run_redis cephfs
run_sqlite cephfs

run_rocksdb ext4
run_redis ext4
run_sqlite ext4
