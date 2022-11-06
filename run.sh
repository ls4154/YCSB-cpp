#!/bin/bash

thread=$1
count=$2
path=$3

rm -rf $path
./ycsb -load -db rocksdb -P workloads/workloadw -P rocksdb/rocksdb.properties -s -threads $thread -p recordcount=$count -p workloadpath=/data/runw -p rocksdb.dbname=$path