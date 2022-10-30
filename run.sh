#!/bin/bash

./ycsb -load -db rocksdb -P workloads/workloadw -P rocksdb/rocksdb.properties -s -threads 1 -p recordcount=10 -p workloadpath=/file/runw
