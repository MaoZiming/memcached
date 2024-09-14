#!/bin/bash
DATASETS=("Meta" "Twitter" "IBM" "Alibaba" "Tencent")
DIR="/home/maoziming/memcached/cache/bench"
# Variables
cd /home/maoziming/memcached/cache/build
make -j


for DATASET in "${DATASETS[@]}"; do
    for sketches in SketchesTracker MinSketchTracker; do
    ./client/sketches $DATASET $sketches > $DIR/sketches/$DATASET\_$sketches.log &
    ./client/sketches $DATASET $sketches > $DIR/sketches/$DATASET\_$sketches.log &
    done
done