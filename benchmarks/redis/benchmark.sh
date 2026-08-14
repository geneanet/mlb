#!/bin/bash

# Change to the script's directory to find docker-compose.yml
cd "$(dirname "$0")"

# Results storage
RESULTS=""

# Cleanup function to shutdown services and show summary
cleanup() {
    echo "--- Shutting down services ---"
    docker-compose down
    
    if [ -n "$RESULTS" ]; then
        echo -e "\n========================================================================================"
        echo "                         VALKEY BENCHMARK SUMMARY"
        echo "========================================================================================"
        printf "%-15s | %-10s | %-8s | %-8s | %-8s\n" "Target" "Ops/sec" "Avg Lat" "p50 Lat" "p99 Lat"
        echo "----------------------------------------------------------------------------------------"
        echo -e "$RESULTS" | while IFS='|' read -r target ops avg p50 p99; do
            if [ -n "$target" ]; then
                printf "%-15s | %-10s | %-8s | %-8s | %-8s\n" "$target" "$ops" "$avg" "$p50" "$p99"
            fi
        done
        echo "========================================================================================"
    fi
}

# Ensure cleanup is run on exit
trap cleanup EXIT

echo "--- Building mlb ---"
(cd ../.. && CGO_ENABLED=0 go build -o mlb .)

echo "--- Starting services ---"
docker-compose down 
docker-compose up -d --build

# wait for services to be ready
echo "--- Waiting for services to start ---"
sleep 5

run_bench() {
    NAME=$1
    SERVER=$2
    PORT=$3
    TYPE=$4 # "throughput" or "conn-rate"

    # Use 4 threads, 25 clients per thread = 100 total connections
    THREADS=4
    CLIENTS=25

    if [ "$TYPE" == "throughput" ]; then
        echo "--- Benchmarking $NAME (200 total concurrent) ---"
        OUT=$(docker-compose exec -T bench-client memtier_benchmark -s $SERVER -p $PORT -P redis --ratio=10:1 -c $CLIENTS -t $THREADS --test-time=10 --reconnect-on-error --max-reconnect-attempts=100 --ipv4 --hide-histogram)
        LABEL="$NAME (thrpt)"
    else
        echo "--- Benchmarking $NAME (High Connection Rate, 5 requests per connection, 200 total concurrent) ---"
        OUT=$(docker-compose exec -T bench-client memtier_benchmark -s $SERVER -p $PORT -P redis --ratio=10:1 -c $CLIENTS -t $THREADS --reconnect-interval=5 --test-time=10 --reconnect-on-error --max-reconnect-attempts=100 --ipv4 --hide-histogram)
        LABEL="$NAME (conn)"
    fi
    
    echo "$OUT"

    # Extract metrics from the "Totals" line
    METRICS=$(echo "$OUT" | grep "Totals" | awk '{printf "%s|%s|%s|%s", $2, $5, $6, $7}')
    RESULTS="$RESULTS$LABEL|$METRICS\n"
}

# # 1. Throughput test (Persistent connections)
run_bench "valkey-direct" "valkey1" "6379" "throughput"
run_bench "mlb-redis" "mlb" "6380" "throughput"
run_bench "mlb-tcp" "mlb" "6382" "throughput"
run_bench "twemproxy" "twemproxy" "6381" "throughput"

# 2. Connection rate test (Reconnect)
run_bench "valkey-direct" "valkey1" "6379" "conn-rate"
run_bench "mlb-redis" "mlb" "6380" "conn-rate"
run_bench "mlb-tcp" "mlb" "6382" "conn-rate"
run_bench "twemproxy" "twemproxy" "6381" "conn-rate"
