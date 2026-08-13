#!/usr/bin/env bash
set -euo pipefail

mkdir -p loadtest/results
timestamp=$(date +%Y%m%d-%H%M%S)

echo "Running ghz against gateway-1 (localhost:50051)..."
ghz --config=loadtest/echo.ghz.json \
    -O json -o "loadtest/results/gateway-1-${timestamp}.json" \
    localhost:50051

echo "Running ghz against gateway-2 (localhost:50052)..."
ghz --config=loadtest/echo.ghz.json \
    -O json -o "loadtest/results/gateway-2-${timestamp}.json" \
    localhost:50052

python loadtest/summarize.py "loadtest/results/gateway-1-${timestamp}.json" "loadtest/results/gateway-2-${timestamp}.json"
