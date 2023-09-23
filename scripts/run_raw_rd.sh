#!/bin/bash

set -x

msg_size=(128 256 512 1024 2048 4096 8192)

ncl_dir=/data/compute-side-log/build/src  # NCL binary and library directory
res_dir=/data/result/raw  # result directory
zkdir=/data/apache-zookeeper-3.6.3-bin  # zookeeper binary directory

user=luoxh
server=hp089.utah.cloudlab.us
replica=(hp009.utah.cloudlab.us)

mkdir -p $res_dir

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
    for r in ${replica[@]}
    do
        ssh -o StrictHostKeyChecking=no $user@$r "nohup $ncl_dir/server > $ncl_dir/server.log 2>&1 &"
    done
    sleep 1
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

function build_for_remote_read() {
    sync_read=$1

    ssh -o StrictHostKeyChecking=no $user@$server "cd /data/compute-side-log ; \
        cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DREMOTE_READ=$sync_read ; \
        cmake --build build -j "
}

function run_cephfs() {
    csv=$res_dir/cephfs_read.csv
    rm $csv
    dd if=/dev/zero of=/mnt/cephfs/test.txt bs=1M count=100
    for ms in ${msg_size[@]}
    do
        echo 3 | sudo tee /proc/sys/vm/drop_caches
        sleep 1
        lat=`$ncl_dir/posix_client $ms r /mnt/cephfs/test.txt normal | grep average | awk '{print $2}'`
        echo $ms,$lat >> $csv
    done
}

function run_direct_io() {
    csv=$res_dir/cephfs_sync_read.csv
    rm $csv
    dd if=/dev/zero of=/mnt/cephfs/test.txt bs=1M count=100
    for ms in ${msg_size[@]}
    do
        lat=`$ncl_dir/posix_client $ms r /mnt/cephfs/test.txt direct 10 | grep average | awk '{print $2}'`
        echo $ms,$lat >> $csv
    done
}

function run_ncl() {
    build_for_remote_read OFF

    run_zk

    run_ncl_server

    csv=$res_dir/ncl_read.csv
    rm $csv
    for ms in ${msg_size[@]}
    do
        $ncl_dir/posix_client $ms w /mnt/cephfs/test.txt prepare
        $ncl_dir/posix_client $ms r /mnt/cephfs/test.txt ncl | tee ./output.txt
        recover=`cat ./output.txt | grep recover | awk '{print $2}'`
        num=`cat ./output.txt | grep num | awk '{print $2}'`
        lat=`cat ./output.txt | grep average | awk '{print $2}'`
        echo $ms,$recover,$num,$lat >> $csv
    done

    kill_ncl_server

    stop_zk
}

function run_ncl_sync() {
    build_for_remote_read ON

    run_zk

    run_ncl_server

    csv=$res_dir/sync_ncl_read.csv
    rm $csv
    for ms in ${msg_size[@]}
    do
        $ncl_dir/posix_client $ms w /mnt/cephfs/test.txt prepare
        lat=`$ncl_dir/posix_client $ms r /mnt/cephfs/test.txt ncl | grep average | awk '{print $2}'`
        echo $ms,$lat >> $csv
    done

    kill_ncl_server

    stop_zk
}

run_cephfs
# run_direct_io
# run_ncl_sync
# run_ncl
