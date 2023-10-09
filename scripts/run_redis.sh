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

user=luoxh
server=hp182.utah.cloudlab.us
client=hp126.utah.cloudlab.us
replica=(hp158.utah.cloudlab.us hp169.utah.cloudlab.us hp160.utah.cloudlab.us)

dir=/data/YCSB-cpp  # YCSB binary directory
ncl_dir=/data/compute-side-log/build/src  # NCL server binary and library directory
zkdir=/data/apache-zookeeper-3.6.3-bin  # zookeeper binary directory
redis_dir=/data/redis  # redis binary directory
db_dir=/mnt/cephfs/ycsb-redis  # redis database directory
db_base=/data/redis_base  # redis base snapshot directory
output_dir=/data/result/redis  # experiemnt results directory
local_output_dir=~/result/redis  # local experiemnt results directory

iter=1

declare -A redis_svr_pid
declare -A ncl_server_pid
redis_svr_pid[$server]=0
for r in ${replica[@]}
do
    ncl_server_pid[$r]=0
    redis_svr_pid[$r]=0
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
    ssh -o StrictHostKeyChecking=no $user@$server "rm -rf $db_dir ; cp -r $db_base $db_dir" &
    cp1=$!

    if [ $backend = app ] then
        ssh -o StrictHostKeyChecking=no $user@${replica[0]} "rm -rf $db_dir ; cp -r $db_base $db_dir" &
        cp2=$!
        ssh -o StrictHostKeyChecking=no $user@${replica[1]} "rm -rf $db_dir ; cp -r $db_base $db_dir" &
        cp3=$!
    fi

    wait $cp1
    if [ $backend = app ] then
        wait $cp2
        wait $cp3
    fi
}

function prepare_load() {
    ssh -o StrictHostKeyChecking=no $user@$server "rm -rf $db_dir/*"
    ssh -o StrictHostKeyChecking=no $user@$server "echo 3 | sudo tee /proc/sys/vm/drop_caches"

    if [ $backend = app ] then
        ssh -o StrictHostKeyChecking=no $user@${replica[0]} "rm -rf $db_dir/*"
        ssh -o StrictHostKeyChecking=no $user@${replica[0]} "echo 3 | sudo tee /proc/sys/vm/drop_caches"
        ssh -o StrictHostKeyChecking=no $user@${replica[1]} "rm -rf $db_dir/*"
        ssh -o StrictHostKeyChecking=no $user@${replica[1]} "echo 3 | sudo tee /proc/sys/vm/drop_caches"
    fi
}

function run_redis_server() {
    if [ $backend = ncl ] || [ $backend = sync_ncl ]; then
        extra_lib="LD_PRELOAD=$ncl_dir/libcsl.so IBV_FORK_SAFE=1"
    fi

    if [ $backend = sync ] || [ $backend = sync_ncl ]; then
        conf="redis_sync.conf"
    else
        conf="redis.conf"
    fi

    kill_redis_server $server || true
    if [ $backend = app ] then
        kill_redis_server ${replica[0]} || true
        kill_redis_server ${replica[1]} || true
    fi

    ssh -o StrictHostKeyChecking=no $user@$server "$extra_lib \
        nohup $redis_dir/src/redis-server $dir/redis/$conf \
        >$redis_dir/redis_svr.log 2>&1" &
    redis_svr_pid[$server]=$!

    if [ $backend = app ]; then
        ssh -o StrictHostKeyChecking=no $user@${replica[0]} " \
            nohup $redis_dir/src/redis-server $redis_dir/redis.conf \
            >$redis_dir/redis_svr.log 2>&1" &
        redis_svr_pid[${replica[0]}]=$!
        ssh -o StrictHostKeyChecking=no $user@${replica[1]} " \
            nohup $redis_dir/src/redis-server $redis_dir/redis.conf \
            >$redis_dir/redis_svr.log 2>&1" &
        redis_svr_pid[${replica[1]}]=$!
    fi

    if [ $mode = run ]
    then
        echo "waiting for loading rdb"
        sleep 1000
        echo "should have finished loading"
    fi
}

function kill_redis_server() {
    ip=$1
    ssh -o StrictHostKeyChecking=no $user@$ip "pid=\$(sudo lsof -i :6379 | awk 'NR==2 {print \$2}') ; \
        sudo kill -2 \$pid 2> /dev/null || true "
    wait ${redis_svr_pid[$ip]}
    ssh -o StrictHostKeyChecking=no $user@$ip "ps aux | grep $redis_dir/src/redis-server"
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

    if [ $backend = ncl ] || [ $backend = sync_ncl ]; then
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

    kill_redis_server $server
    if [ $backend = app ] then
        kill_redis_server ${replica[0]}
        kill_redis_server ${replica[1]}
    fi

    if [ $backend = ncl ] || [ $backend = sync_ncl ]; then
        kill_ncl_server
        stop_zk
    fi
}

function run_ycsb() {
    workloads=(workloada workloadb workloadc workloadd workloadf)
    if [ $backend = sync ]
    then
        operation_M=(10 40 120 40 10)
    else
        operation_M=(200 200 200 200 200)
    fi

    for (( i=1; i<=$iter; i++ ))
        do
        for idx in ${!workloads[@]}
        do
            wl=${workloads[$idx]}
            operationcnt=$((${operation_M[$idx]} * 100000))

            expname=redis_"$wl"_"$backend"_trail"$i"
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
        record_M=(1 1 2 4 6 8 8)
    else
        record_M=(30 70 120 200 200 200 200)
    fi

    for (( i=1; i<=$iter; i++ ))
    do
        for idx in ${!n_threads[@]}
        do
            thread=${n_threads[$idx]}
            recordcnt=$((${record_M[$idx]} * 100000))

            expname=redis_load_"$backend"_th"$thread"_trail"$i"
            echo "Running experiment $expname"
            run_once load workloadw $thread $recordcnt 0 $db_dir $expname
        done
    done
}

if [ $mode = load ]; then
    run_load
elif [ $mode = run ]
    run_ycsb
else
    echo "Unknown mode"
fi

echo "end at $(date '+%B %d %Y %T')"
