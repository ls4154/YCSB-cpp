#!/bin/bash

set -ex

thread=$1
count=$2
path=$3

rm -rf $path
mkdir -p $thread

python wal_mon.py mon $path/wal $thread/wal.yml &
mon_pid=$!


./ycsb -load -db rocksdb -P workloads/workloadw -P rocksdb/rocksdb.properties -s -threads $thread -p recordcount=$count -p workloadpath=/data/runw -p rocksdb.dbname=$path 2>&1 | tee $thread/ycsb.log

kill -2 $mon_pid

mv $path/LOG $thread
