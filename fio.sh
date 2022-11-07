#!/bin/bash

set -ex

typ=$1
folder=/$typ/tmp

bs=(2 4 8 16 32 64 128 256 512)

for b in "${bs[@]}"
do
	rm ${folder}/test.0.0
	fio -directory=$folder -direct=0 -rw=write -ioengine=sync -bs=${b}M -size=2G -numjobs=1 -name test -fdatasync=1 2>&1 | tee ${folder}/${typ}_${b}.log 
done
