#!/bin/bash

# Performance Comparison Script for Byzantine Verification
# Usage: ./run_performance_comparison.sh [ExampleName]
# Example: ./run_performance_comparison.sh MerkleVsFullTaskComparison

EXAMPLE="${1:-MerkleVsFullTaskComparison}"  # Default to MerkleVsFullTaskComparison if not specified
RESULTS_CSV="byzantine_performance_${EXAMPLE}.csv"

echo "========================================="
echo "BYZANTINE VERIFICATION PERFORMANCE TEST"
echo "Testing Example: $EXAMPLE"
echo "========================================="
echo "Results will be saved to: $RESULTS_CSV"
echo ""

# Create CSV header
echo "Config,HONEST,BYZANTINE_PROB,EXEC_VERIFICATION,MERKLE_VERIFICATION,MERKLE_BUILD_ON_DRIVER,ExecutionTime_seconds,Status" > "$RESULTS_CSV"

# Function to run test and extract timing
run_performance_test() {
    local config_name=$1
    shift
    local env_vars="$@"
    
    echo ""
    echo "========================================="
    echo "[$config_name] Running: $EXAMPLE"
    echo "Environment: $env_vars"
    echo "========================================="
    
    local start_time=$(date +%s)
    
    # Run the example and capture output
    local output=$(env $env_vars ./bin/run-example $EXAMPLE 2>&1)
    local exit_code=$?
    
    local end_time=$(date +%s)
    local wall_time=$((end_time - start_time))
    
    # Extract timing from output (different formats for different examples)
    local exec_time=""
    if echo "$output" | grep -q "Total execution time:"; then
        # MerkleVsFullTaskComparison format: "Total execution time: 45.67 seconds"
        exec_time=$(echo "$output" | grep "Total execution time:" | sed -E 's/.*Total execution time: ([0-9.]+).*/\1/')
    elif echo "$output" | grep -q "Total time:"; then
        # PerformanceTest format: "Total time: 12.5s"
        exec_time=$(echo "$output" | grep "Total time:" | sed -E 's/.*Total time: ([0-9.]+)s.*/\1/')
    else
        # Fallback to wall clock time
        exec_time="$wall_time"
    fi
    
    # Determine status
    local status="FAIL"
    if [ $exit_code -eq 0 ]; then
        if echo "$output" | grep -q "SUCCESS\|RESULTS"; then
            status="PASS"
        fi
    fi
    
    # Parse environment variables
    local honest=$(echo "$env_vars" | grep -oP 'HONEST=\K\w+' || echo "N/A")
    local byz_prob=$(echo "$env_vars" | grep -oP 'BYZANTINE_PROBABILITY=\K\d+' || echo "N/A")
    local exec_verif=$(echo "$env_vars" | grep -oP 'EXEC_VERIFICATION=\K\w+' || echo "N/A")
    local merkle_verif=$(echo "$env_vars" | grep -oP 'MERKLE_VERIFICATION=\K\w+' || echo "N/A")
    local merkle_driver=$(echo "$env_vars" | grep -oP 'MERKLE_BUILD_ON_DRIVER=\K\w+' || echo "N/A")
    
    # Log to CSV
    echo "$config_name,$honest,$byz_prob,$exec_verif,$merkle_verif,$merkle_driver,$exec_time,$status" >> "$RESULTS_CSV"
    
    echo ""
    echo "Result: $status | Time: ${exec_time}s"
    echo ""
    
    # Also print relevant output sections
    echo "$output" | grep -E "RESULTS|Total.*time|SUCCESS|FAIL|Shuffle.*Verification|Configuration:" | head -20
}

# BASELINE - No Byzantine faults
run_performance_test "0-Baseline" \
    "HONEST=True"

echo ""
echo "--- GROUP A: EXECUTOR VERIFICATION ---"

# 1a) Executor - Full Task
run_performance_test "1a-Executor-FullTask" \
    "HONEST=False" "BYZANTINE_PROBABILITY=10" \
    "EXEC_VERIFICATION=true" "MERKLE_VERIFICATION=false"

# 1b) Executor - Merkle
run_performance_test "1b-Executor-Merkle" \
    "HONEST=False" "BYZANTINE_PROBABILITY=10" \
    "EXEC_VERIFICATION=true" "MERKLE_VERIFICATION=true"

echo ""
echo "--- GROUP B: DRIVER VERIFICATION ---"

# 2a) Driver - Full Task
run_performance_test "2a-Driver-FullTask" \
    "HONEST=False" "BYZANTINE_PROBABILITY=10" \
    "EXEC_VERIFICATION=false" "MERKLE_VERIFICATION=false"

# 2b) Driver - Merkle (Trees on Driver)
run_performance_test "2b-Driver-Merkle-TreesOnDriver" \
    "HONEST=False" "BYZANTINE_PROBABILITY=10" \
    "EXEC_VERIFICATION=false" "MERKLE_VERIFICATION=true" \
    "MERKLE_BUILD_ON_DRIVER=true"

# 2c) Driver - Merkle (Trees on Executors)
run_performance_test "2c-Driver-Merkle-TreesOnExecutors" \
    "HONEST=False" "BYZANTINE_PROBABILITY=10" \
    "EXEC_VERIFICATION=false" "MERKLE_VERIFICATION=true" \
    "MERKLE_BUILD_ON_DRIVER=false"

echo ""
echo "========================================="
echo "ALL TESTS COMPLETED FOR $EXAMPLE"
echo "========================================="
echo "Results saved to: $RESULTS_CSV"
echo ""
echo "Summary of comparisons to analyze:"
echo ""
echo "1. Merkle Efficiency (Executor):"
echo "   - Compare: 1a vs 1b"
echo ""
echo "2. Merkle Efficiency (Driver):"
echo "   - Compare: 2a vs 2b/2c"
echo ""
echo "3. Tree Building Location:"
echo "   - Compare: 2b vs 2c"
echo ""
echo "4. Executor vs Driver (Full Task):"
echo "   - Compare: 1a vs 2a"
echo ""
echo "5. Executor vs Driver (Merkle):"
echo "   - Compare: 1b vs 2b/2c"
echo ""
echo "6. Verification Overhead:"
echo "   - Compare: Baseline vs all others"
echo ""

# Display CSV results
echo "========================================="
echo "CSV Results:"
echo "========================================="
column -t -s',' "$RESULTS_CSV"
