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
redis_dir=/data/redis  # redis binary directory
db_dir=/mnt/cephfs/ycsb-redis  # redis database directory
db_base=/data/redis_base  # redis base snapshot directory
output_dir=/data/result/redis  # experiemnt results directory
local_output_dir=~/result/redis  # local experiemnt results directory

redis_svr_pid=0
declare -A ncl_server_pid
for r in ${replica[@]}
do
    ncl_server_pid[$r]=0
    echo ${ncl_server_pid[$r]}
done

function prepare_run() {
    # When aof is enabled redis will load from aof on start, however I don't want it to load from aof since the aof file
    # is on the remote server. Since the full snapshopt is also saved locally as dump.rdb, I want redis to load from that.
    # To do so, I fool redis by substituding the aof base rdb with the full snapshot rdb and truncating the incr aof to 0,
    # so redis can load the full database from rdb.
    # ssh -o StrictHostKeyChecking=no $user@$server "base=\$(find $db_dir/appendonlydir/ -name 'appendonly.aof.*.base.rdb') ; \
    #     cp $db_dir/dump.rdb \$base ; \
    #     aof=\$(find $db_dir/appendonlydir/ -name 'appendonly.aof.*.incr.aof'); \
    #     truncate -s 0 \$aof"
    ssh -o StrictHostKeyChecking=no $user@$server "rm -rf $db_dir ; cp -r $db_base $db_dir"
    ssh -o StrictHostKeyChecking=no $user@$server "echo 3 | sudo tee /proc/sys/vm/drop_caches"
}

function prepare_load() {
    ssh -o StrictHostKeyChecking=no $user@$server "rm -rf $db_dir/*"
    ssh -o StrictHostKeyChecking=no $user@$server "echo 3 | sudo tee /proc/sys/vm/drop_caches"
}

function run_redis_server() {
    if [ $backend = ncl ]; then
        extra_lib="LD_PRELOAD=$ncl_dir/libcsl.so"
    fi

    kill_redis_server || true

    ssh -o StrictHostKeyChecking=no $user@$server "$extra_lib \
        nohup $redis_dir/src/redis-server $dir/redis/redis.conf \
        >$redis_dir/redis_svr.log 2>&1" &
    redis_svr_pid=$!
    echo "waiting for loading rdb"
    sleep 180
    echo "should have finished loading"
}

function kill_redis_server() {
    ssh -o StrictHostKeyChecking=no $user@$server "pid=\$(sudo lsof -i :6379 | awk 'NR==2 {print \$2}') ; \
        sudo kill -2 \$pid 2> /dev/null || true "
    wait $redis_svr_pid
    ssh -o StrictHostKeyChecking=no $user@$server "ps aux | grep $dir/redis-server"
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

function run_redis_cli() {
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
        extra_flag=
    fi

    ssh -o StrictHostKeyChecking=no $user@$client "mkdir -p $output_dir"
    
    ssh -o StrictHostKeyChecking=no $user@$client "$dir/ycsb -$mode -db redis \
        -P $dir/workloads/$wl \
        -P $dir/redis/redis.properties \
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
        prepare_load
    else
        prepare_run
    fi

    run_redis_server

    sleep 15

    run_redis_cli $mode $workload $thread $recordcount $operationcount $path $yaml

    kill_redis_server

    if [ $backend = ncl ]; then
        kill_ncl_server
        stop_zk
    fi
}

function run_ycsb() {
    iter=3
    workloads=(workloada workloadb workloadc workloadd workloadf)
    operation_M=(20 20 20 20 20)

    for (( i=1; i<=$iter; i++ ))
        do
        for idx in ${!workloads[@]}
        do
            wl=${workloads[$idx]}
            operationcnt=$((${operation_M[$idx]} * 1000000))

            expname=redis_"$wl"_"$backend"_trail"$i"
            echo "Running experiment $expname"
            sleep 1
            run_once run $wl 20 100000000 $operationcnt $db_dir $expname
        done
    done
}

function run_load() {
    iter=3

    n_threads=(1 2 4 8 12 16)
    record_M=(30 70 120 200 200 200)

    # n_threads=(20)
    # record_M=(200)

    for idx in ${!n_threads[@]}
    do
        for (( i=1; i<=$iter; i++ ))
        do
            thread=${n_threads[$idx]}
            recordcnt=$((${record_M[$idx]} * 100000))

            expname=redis_load_"$backend"_th"$thread"_trail"$i"
            echo "Running experiment $expname"
            run_once load workloadw $thread $recordcnt 0 $db_dir $expname
        done
    done
}

# run_load
run_ycsb

echo "end at $(date '+%B %d %Y %T')"
