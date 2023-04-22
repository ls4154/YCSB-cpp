#!/bin/bash

date=$(date +"%Y%m%d%s")
exec > >(tee scripts/log/"$date"_experiment.log) 2>&1

echo "start at $(date '+%B %d %Y %T')"

set -ex

if [ $# -ge 1 ]; then
    backend=$1
else
    backend=cephfs
fi

user=luoxh
server=hp140.utah.cloudlab.us
client=hp155.utah.cloudlab.us
replica=(hp101.utah.cloudlab.us hp099.utah.cloudlab.us hp129.utah.cloudlab.us)

dir=/data/YCSB-cpp  # YCSB binary directory
ncl_dir=/data/compute-side-log/build/src  # NCL server binary and library directory
zkdir=/data/apache-zookeeper-3.6.3-bin  # zookeeper binary directory
db_dir=/mnt/cephfs/ycsb-sqlite/ycsb.db  # sqlite database directory
db_base=/data/sqlite_base/ycsb.db  # sqlite base snapshot directory
output_dir=/data/result/sqlite  # experiemnt results directory
local_output_dir=~/result/sqlite  # local experiemnt results directory

sqlite_svr_pid=0
declare -A ncl_server_pid
for r in ${replica[@]}
do
    ncl_server_pid[$r]=0
    echo ${ncl_server_pid[$r]}
done

function prepare_run() {
    ssh -o StrictHostKeyChecking=no $user@$server "echo 600M | sudo tee /sys/fs/cgroup/memory/ycsb/memory.limit_in_bytes"
    ssh -o StrictHostKeyChecking=no $user@$server "echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh -o StrictHostKeyChecking=no $user@$server "sudo rm -f $db_dir ; cgexec -g memory:ycsb cp -r $db_base $db_dir"
}

function prepare_load() {
    memlim=$1
    echo "mem limit ${memlim}B"
    # ssh -o StrictHostKeyChecking=no $user@$server "echo $memlim | sudo tee /sys/fs/cgroup/memory/ycsb/memory.limit_in_bytes"
    # ssh -o StrictHostKeyChecking=no $user@$server "echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh -o StrictHostKeyChecking=no $user@$server "sudo rm -f $db_dir"
}

function run_sqlite_server() {
    mode=$1
    th=$2
    if [ $backend = ncl ]; then
        extra_lib="LD_PRELOAD=$ncl_dir/libcsl.so"
    fi

    kill_sqlite_server || true

    ssh -o StrictHostKeyChecking=no $user@$server "sudo $extra_lib \
        nohup cgexec -g memory:ycsb \
        $dir/sqlite_svr \
        -P $dir/sqlite/sqlite.properties \
        -p sqlite.dbname=$db_dir \
        -threads $th >$dir/sqlite_svr.log 2>&1" &
    sqlite_svr_pid=$!
}

function kill_sqlite_server() {
    ssh -o StrictHostKeyChecking=no $user@$server "pid=\$(sudo lsof -i :31851 | awk 'NR==2 {print \$2}') ; \
        sudo kill -2 \$pid 2> /dev/null || true "
    wait $sqlite_svr_pid
    ssh -o StrictHostKeyChecking=no $user@$server "ps aux | grep $dir/sqlite_svr"
}

function run_zk() {
    stop_zk || true
    ssh -o StrictHostKeyChecking=no $user@$server "rm -rf $zkdir/data; $zkdir/bin/zkServer.sh start"
}

function stop_zk() {
    ssh -o StrictHostKeyChecking=no $user@$server "$zkdir/bin/zkServer.sh stop"
}

function run_ncl_server() {
    kill_ncl_server || true
    for r in ${replica[@]}
    do
        ssh -o StrictHostKeyChecking=no $user@$r "nohup $ncl_dir/server > $ncl_dir/server.log 2>&1 &"
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

function run_sqlite_cli() {
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
        extra_flag="-p secskip=60"
    fi

    ssh -o StrictHostKeyChecking=no $user@$client "mkdir -p $output_dir"
    
    ssh -o StrictHostKeyChecking=no $user@$client "sudo $dir/ycsb -$mode -db sqlite \
        -P $dir/workloads/$wl \
        -P $dir/sqlite/sqlite.properties \
        -s -threads $th \
        -p recordcount=$rccnt \
        -p operationcount=$opcnt \
        -p measurementtype=basic \
        -p yamlname=$output_dir/$yaml \
        $extra_flag"

    mkdir -p $local_output_dir/$mode
    scp $user@$client:$output_dir/$yaml.yml $local_output_dir/$mode/
}

function run_once() {
    mode=$1
    workload=$2
    thread=$3
    recordcount=$4
    operationcount=$5
    path=$6
    yaml=$7

    if [ $backend = ncl ]; then
        run_zk
        run_ncl_server
    fi

    if [ $mode = load ]
    then
        prepare_load $(printf "%.0f" "$(echo "scale=0; $recordcount*128*0.3" | bc)")
    else
        prepare_run
    fi

    run_sqlite_server $mode $thread

    sleep 15

    run_sqlite_cli $mode $workload $thread $recordcount $operationcount $path $yaml

    kill_sqlite_server

    if [ $backend = ncl ]; then
        kill_ncl_server
        stop_zk
    fi
}

function run_ycsb() {
    iter=3
    workloads=(workloada workloadb workloadc workloadd workloadf)
    operation_M=(5 6 6 10 6)

    for (( i=1; i<=$iter; i++ ))
    do
        for idx in ${!workloads[@]}
        do
            wl=${workloads[$idx]}
            operationcnt=$((${operation_M[$idx]} * 100000))

            expname=sqlite_"$wl"_"$backend"_trail"$i"
            echo "Running experiment $expname"
            sleep 1
            run_once run $wl 1 10000000 $operationcnt $db_dir $expname
        done
    done
}

function run_load() {
    iter=3

    n_threads=(1)
    record_M=(20)

    # n_threads=(16 20)
    # record_M=(70 50)

    for idx in ${!n_threads[@]}
    do
        for (( i=1; i<=$iter; i++ ))
        do
            thread=${n_threads[$idx]}
            recordcnt=$((${record_M[$idx]} * 1000000))

            expname=sqlite_load_"$backend"_th"$thread"_trail"$i"
            echo "Running experiment $expname"
            run_once load workloadw $thread $recordcnt 0 $db_dir $expname
        done
    done
}

# run_load
run_ycsb

echo "end at $(date '+%B %d %Y %T')"
