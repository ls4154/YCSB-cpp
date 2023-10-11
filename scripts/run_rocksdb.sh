#!/bin/bash

date=$(date +"%Y%m%d%s")
exec > >(tee scripts/log/"$date"_experiment.log) 2>&1

echo "start at $(date '+%B %d %Y %T')"

set -ex

if [ $# -ge 1 ]; then
    backend=$1
    mode=$2
else
    backend=cephfs
    mode=$1
fi

source $(dirname "$0")/config.sh
rocksdb_dir=/data/rocksdb  # rocksdb library directory
db_dir=/mnt/cephfs/ycsb-rocksdb  # rocksdb database directory
db_base=/data/db_base_stable  # rocksdb base snapshot directory
output_dir=/data/result/rocksdb  # experiemnt results directory
local_output_dir=~/result/rocksdb  # local experiemnt results directory

iter=1

rocksdb_svr_pid=0
declare -A ncl_server_pid
for r in ${replica[@]}
do
    ncl_server_pid[$r]=0
    echo ${ncl_server_pid[$r]}
done

function prepare_run() {
    ssh -o StrictHostKeyChecking=no $user@$server "echo 3G | sudo tee /sys/fs/cgroup/memory/ycsb/memory.limit_in_bytes"
    ssh -o StrictHostKeyChecking=no $user@$server "echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh -o StrictHostKeyChecking=no $user@$server "sudo rm -rf $db_dir/ ; cgexec -g memory:ycsb cp -r $db_base $db_dir"
}

function prepare_load() {
    if [ $backend = sync ]; then
        memlim=-1
    else
        memlim=$1
    fi
    echo "mem limit ${memlim}B"
    ssh -o StrictHostKeyChecking=no $user@$server "echo $memlim | sudo tee /sys/fs/cgroup/memory/ycsb/memory.limit_in_bytes"
    ssh -o StrictHostKeyChecking=no $user@$server "echo 3 | sudo tee /proc/sys/vm/drop_caches"
}

function run_rocksdb_server() {
    mode=$1
    th=$2
    extra_flag=""

    if [ $backend = ncl ] || [ $backend = sync_ncl ]; then
        extra_lib="LD_PRELOAD=$ncl_dir/libcsl.so"
    fi

    if [ $backend = sync ] || [ $backend = sync_ncl ]; then
        extra_flag="-p rocksdb.sync_wal=true "
    fi

    kill_rocksdb_server || true

    if [ $mode = load ]
    then
        extra_flag+="-p rocksdb.destroy=true"
    else
        extra_flag+="-p rocksdb.destroy=false"
    fi

    ssh -o StrictHostKeyChecking=no $user@$server "sudo LD_LIBRARY_PATH=$rocksdb_dir $extra_lib \
        nohup cgexec -g memory:ycsb \
        $dir/rocksdb_svr \
        -P $dir/rocksdb-clisvr/rocksdb-clisvr.properties \
        -p rocksdb.dbname=$db_dir \
        $extra_flag \
        -threads $th >$dir/rocksdb_svr.log 2>&1" &
    rocksdb_svr_pid=$!
}

function kill_rocksdb_server() {
    ssh -o StrictHostKeyChecking=no $user@$server "pid=\$(sudo lsof -i :31851 | awk 'NR==2 {print \$2}') ; \
        sudo kill -2 \$pid 2> /dev/null || true "
    wait $rocksdb_svr_pid
    ssh -o StrictHostKeyChecking=no $user@$server "ps aux | grep $dir/rocksdb_svr"
}

function run_zk() {
    stop_zk || true
    ssh -o StrictHostKeyChecking=no $user@$server "rm -rf $zkdir/zookeeper; $zkdir/bin/zkServer.sh start"
}

function stop_zk() {
    ssh -o StrictHostKeyChecking=no $user@$server "$zkdir/bin/zkServer.sh stop"
}

function run_ncl_server() {
    kill_ncl_server || true
    for i in ${!replica[@]}
    do
        r=${replica[$i]}
        ssh -o StrictHostKeyChecking=no $user@$r "nohup $ncl_dir/server > $ncl_dir/server$i.log 2>&1 &"
        ncl_server_pid[$r]=$!
    done
}

function kill_ncl_server() {
    for r in ${replica[@]}
    do
        ssh -o StrictHostKeyChecking=no $user@$r "pid=\$(sudo lsof -i :8011 | awk 'NR==2 {print \$2}') ; \
            sudo kill -2 \$pid 2> /dev/null || true "
        # wait ${ncl_server_pid[$r]}
        ssh -o StrictHostKeyChecking=no $user@$r "ps aux | grep $ncl_dir/server"
    done
    sleep 1
}

function run_rocksdb_cli() {
    mode=$1
    wl=$2
    th=$3
    rccnt=$4
    opcnt=$5
    path=$6
    yaml=$7

    if [ $mode = load ]
    then
        extra_flag="-p workloadpath=/data/runw"
    else
        extra_flag="-p secskip=30"
    fi

    ssh -o StrictHostKeyChecking=no $user@$client "mkdir -p $output_dir/$mode/$backend"
    
    ssh -o StrictHostKeyChecking=no $user@$client "sudo $dir/ycsb -$mode -db rocksdbcli \
        -P $dir/workloads/$wl \
        -P $dir/rocksdb-clisvr/rocksdb-clisvr.properties \
        -s -threads $th \
        -p recordcount=$rccnt \
        -p operationcount=$opcnt \
        -p measurementtype=basic \
        -p yamlname=$output_dir/$mode/$backend/$yaml \
        $extra_flag"

    mkdir -p $local_output_dir/$mode/$backend
    scp $user@$client:$output_dir/$mode/$backend/$yaml.yml $local_output_dir/$mode/$backend
}

function run_once() {
    mode=$1
    workload=$2
    thread=$3
    recordcount=$4
    operationcount=$5
    path=$6
    yaml=$7

    if [ $backend = ncl ] || [ $backend = sync_ncl ]; then
        run_zk
        run_ncl_server
    fi

    if [ $mode = load ]
    then
        prepare_load $(printf "%.0f" "$(echo "scale=0; $recordcount*128*0.3" | bc)")
    else
        prepare_run
    fi

    run_rocksdb_server $mode $thread

    sleep 30

    run_rocksdb_cli $mode $workload $thread $recordcount $operationcount $path $yaml

    kill_rocksdb_server
 
    if [ $backend = ncl ] || [ $backend = sync_ncl ]; then
        kill_ncl_server
        stop_zk
    fi
}

function run_ycsb() {
    workloads=(workloada workloadb workloadc workloadd workloadf)

    if [ $backend = sync ]
    then
        operation_M=(2 8 8 8 2)
    else
        operation_M=(20 12 12 20 12)
    fi

    for (( i=1; i<=$iter; i++ ))
    do
        for idx in ${!workloads[@]}
        do
            wl=${workloads[$idx]}
            operationcnt=$((${operation_M[$idx]} * 1000000))

            expname=rocksdb_"$wl"_"$backend"_trail"$i"
            echo "Running experiment $expname"
            sleep 1
            run_once run $wl 20 100000000 $operationcnt $db_dir $expname
        done
    done
}

function run_load() {
    n_threads=(1 2 4 8 12 16 20)
    if [ $backend = sync ]
    then
        record_M=(10 10 18 36 54 72 80)
    else
        record_M=(2000 3000 5000 8000 7000 7000 5000)
    fi

    for (( i=1; i<=$iter; i++ ))
    do
        for idx in ${!n_threads[@]}
        do
            thread=${n_threads[$idx]}
            recordcnt=$((${record_M[$idx]} * 10000))

            expname=rocksdb_load_"$backend"_th"$thread"_trail"$i"
            echo "Running experiment $expname"
            run_once load workloadw $thread $recordcnt 0 $db_dir $expname
        done
    done
}

if [ $mode = load ]; then
    run_load
elif [ $mode = run ]; then
    run_ycsb
else
    echo "Unknown mode"
fi

echo "end at $(date '+%B %d %Y %T')"
