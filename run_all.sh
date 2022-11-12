#!/bin/bash

set -ex

folder=$1

threads=(1 2 4 8 12 16 20 24)

for t in "${threads[@]}"
do
    ./run.sh $t $((t * 2500000)) $folder
done
