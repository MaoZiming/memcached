#!/bin/bash
DATASETS=("Meta" "Twitter" "IBM" "Alibaba" "Tencent")

# DATASETS=("Poisson" "PoissonWrite" "PoissonMix")

DIR="/home/maoziming/memcached/cache/bench"
# Variables
cd /home/maoziming/memcached/cache/build
make -j


for DATASET in "${DATASETS[@]}"; do
    for sketches in TopKSketchTracker MinSketchTracker ExactRWTracker; do
        ./client/sketches $DATASET $sketches > $DIR/sketches/$DATASET\_$sketches.log
    done
done

cd /home/maoziming/memcached/cache/bench
python parse_tracker.py