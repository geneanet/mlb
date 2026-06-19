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
        echo "                         BENCHMARK SUMMARY (Mixed GET/SET 30:1)"
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

echo "--- Starting services ---"
docker-compose down 
docker-compose up -d --build

# wait for services to be ready
echo "--- Waiting for services to start ---"
sleep 2

run_bench() {
    NAME=$1
    SERVER=$2
    PORT=$3

    echo "--- Benchmarking $NAME (Mixed GET/SET 30:1) ---"
    # Set ratio to 30:1 for GET:SET, using 1 thread
    OUT=$(docker-compose exec -T bench-client memtier_benchmark -s $SERVER -p $PORT -P memcache_text --ratio=30:1 -c 16 -t 1 -n 10000 --hide-histogram)
    echo "$OUT"

    # Extract metrics from the "Totals" line
    # Format: Totals      <ops/sec>     <hits/sec>   <misses/sec>    <avg>     <p50>     <p99>   <p99.9>       <kb/sec>
    METRICS=$(echo "$OUT" | grep "Totals" | awk '{printf "%s|%s|%s|%s", $2, $5, $6, $7}')

    RESULTS="$RESULTS$NAME|$METRICS\n"
}

run_bench "mlb" "mlb" "11212"
run_bench "twemproxy" "twemproxy" "11213"
run_bench "memcached-proxy" "memcached-proxy" "11214"
