#!/bin/bash

# Variables
CACHE_VM_MEMCACHED_PORT=11211
CACHE_VM_SERVER_PATH="/home/maoziming/memcached/cache/build/cache/server"
CACHE_VM_CLIENT_PATH="/home/maoziming/memcached/cache/build/client"
DB_VM_USER="maoziming"
DB_VM_IP="10.128.0.33"
CACHE_VM_IP="10.128.0.39"
DB_VM_KEY="/home/maoziming/memcached/cache/key"
DB_VM_SERVER_PATH="/home/maoziming/rocksdb/backend/build/server/db_server"
DB_VM_DB_PATH="/home/maoziming/rocksdb/backend/build/test.db"
DB_VM_LOG_DIR="/home/maoziming/rocksdb/backend/build/logs"
CACHE_VM_LOG_DIR="/home/maoziming/memcached/cache/build/logs"
DB_VM_SSH="ssh -i $DB_VM_KEY $DB_VM_USER@$DB_VM_IP"
CACHE_VM_SSH="ssh -i $DB_VM_KEY $DB_VM_USER@$CACHE_VM_IP"
BENCHMARKS=("ttl_bench" "adaptive_bench" "invalidate_bench" "update_bench" "stale_bench")
# BENCHMARKS=("ttl_bench" "stale_bench" "adaptive_bench")
DATASETS=("IBM") # 100 is about right. 
# SCALES=("100" "90" "80" "70" "60" "50" "40" "30")
SCALES=("100")
cd /home/maoziming/memcached/cache/build/
make -j

# Ensure the log directory exists on DB VM
echo "Creating log directory on DB VM..."
$DB_VM_SSH "mkdir -p $DB_VM_LOG_DIR"
if [ $? -ne 0 ]; then
    echo "Error creating log directory on DB VM"
    exit 1
fi

$CACHE_VM_SSH "pkill server"
$DB_VM_SSH "pkill server"
$CACHE_VM_SSH "pkill memcached"

# Loop over each benchmark and dataset combination
for DATASET in "${DATASETS[@]}"; do
    for BENCHMARK in "${BENCHMARKS[@]}"; do
        for SCALE in "${SCALES[@]}"; do
            # Step 1: Start fresh memcached on Cache VM
            echo "Starting fresh memcached on Cache VM..."
            $CACHE_VM_SSH "sudo pkill memcached"
            sleep 5
            $CACHE_VM_SSH "sudo memcached -m 10000 -p $CACHE_VM_MEMCACHED_PORT -u maoziming" &
            if [ $? -ne 0 ]; then
                echo "Error starting memcached for $BENCHMARK with dataset $DATASET"
                exit 1
            fi

            # Step 2: Start fresh cache server on Cache VM
            echo "Starting fresh cache server..."
            $CACHE_VM_SSH "sudo pkill server"
            sleep 5
            $CACHE_VM_SSH "cd /home/maoziming/memcached/cache/build && make -j"
            $CACHE_VM_SSH "$CACHE_VM_SERVER_PATH" &
            if [ $? -ne 0 ]; then
                echo "Error starting cache server for $BENCHMARK with dataset $DATASET"
                $CACHE_VM_SSH "pkill memcached"
                exit 1
            fi

            # Create dynamic log path for each configuration
            LOG_PATH="${DB_VM_LOG_DIR}/${BENCHMARK}_${DATASET}_scale${SCALE}_$(date +%Y%m%d_%H%M%S).log"
            CACHE_LOG_PATH="${CACHE_VM_LOG_DIR}/${BENCHMARK}_${DATASET}_scale${SCALE}_$(date +%Y%m%d_%H%M%S).log"
            echo "Running configuration: $BENCHMARK with dataset $DATASET"

            # Step 3: SSH into DB VM, and start DB server with dynamic log path
            echo "Starting DB server on DB VM with log path $LOG_PATH..."
            $DB_VM_SSH "sudo pkill server"
            $DB_VM_SSH "cd /home/maoziming/rocksdb/backend/build && make -j"
            sleep 5
            $DB_VM_SSH "cd /home/maoziming/rocksdb/backend/build && rm -r test.db && $DB_VM_SERVER_PATH 50051 test.db $LOG_PATH" &
            if [ $? -ne 0 ]; then
                echo "Error starting DB server for $BENCHMARK with dataset $DATASET"
                $CACHE_VM_SSH "pkill server"
                $CACHE_VM_SSH "pkill memcached"
                exit 1
            fi

            # Step 4: Run client benchmark on Cache VM
            sleep 5
            echo "Running $BENCHMARK on Cache VM with dataset $DATASET with log path $CACHE_LOG_PATH..."
            $CACHE_VM_CLIENT_PATH/$BENCHMARK $DATASET $SCALE TopKSketchTracker $CACHE_LOG_PATH
            if [ $? -ne 0 ]; then
                echo "Error running $BENCHMARK with dataset $DATASET"
                $DB_VM_SSH "pkill server"
                $CACHE_VM_SSH "pkill server"
                $CACHE_VM_SSH "pkill memcached"
                # exit 1
            fi

            # Step 5: Tear down DB server for this run
            sleep 10
            echo "Tearing down DB server for $BENCHMARK with dataset $DATASET..."
            $DB_VM_SSH "pkill -f '$DB_VM_SERVER_PATH'"
            if [ $? -ne 0 ]; then
                echo "Error tearing down DB server for $BENCHMARK with dataset $DATASET"
                $CACHE_VM_SSH "pkill server"
                $CACHE_VM_SSH "pkill memcached"
                exit 1
            fi

            echo "Completed $BENCHMARK with dataset $DATASET. Logs saved at $LOG_PATH."
            sleep 5

            # Step 6: Tear down cache server and memcached after each experiment
            echo "Tearing down cache server and memcached..."
            $CACHE_VM_SSH "pkill server"
            $CACHE_VM_SSH "pkill memcached"

            sleep 1 
            # Step 6: Run the plot script after each experiment
            echo "Running plot script for $BENCHMARK with dataset $DATASET..."
            $DB_VM_SSH "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rocksdb && cd /home/maoziming/rocksdb/backend/plot && python test.py"

            cd /home/maoziming/memcached/cache/plot
            python test.py
            python latency_vs_throughput.py
            cd -
        done
    done
done

echo "All benchmarks completed successfully."
