#!/bin/bash

# Comprehensive test suite for Byzantine fault-tolerant Spark
# Tests all combinations of environment variables and tracks timing

RESULTS_FILE="/tmp/spark_test_results.txt"
> "$RESULTS_FILE"  # Clear file

echo "========================================" | tee -a "$RESULTS_FILE"
echo "SPARK BYZANTINE VERIFICATION TEST SUITE" | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

run_test() {
    local test_name=$1
    local example=$2
    shift 2
    local env_vars="$@"
    
    echo "-----------------------------------" | tee -a "$RESULTS_FILE"
    echo "Test: $test_name" | tee -a "$RESULTS_FILE"
    echo "Example: $example" | tee -a "$RESULTS_FILE"
    echo "ENV: $env_vars" | tee -a "$RESULTS_FILE"
    echo "-----------------------------------" | tee -a "$RESULTS_FILE"
    
    # Run and time the test
    start_time=$(date +%s.%N)
    
    if [ -z "$env_vars" ]; then
        timeout 60s ./bin/run-example "$example" 2>&1 | grep -E "CONSENSUS|VERIFICATION|BYZANTINE|Final Output|Group.*sum|^[0-9]+$|Job.*finished" | tail -20 | tee -a "$RESULTS_FILE"
        exit_code=${PIPESTATUS[0]}
    else
        timeout 60s env $env_vars ./bin/run-example "$example" 2>&1 | grep -E "CONSENSUS|VERIFICATION|BYZANTINE|Final Output|Group.*sum|^[0-9]+$|Job.*finished" | tail -20 | tee -a "$RESULTS_FILE"
        exit_code=${PIPESTATUS[0]}
    fi
    
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    
    if [ $exit_code -eq 0 ]; then
        echo "✅ PASSED - Duration: ${duration}s" | tee -a "$RESULTS_FILE"
    elif [ $exit_code -eq 124 ]; then
        echo "⏱️  TIMEOUT (60s)" | tee -a "$RESULTS_FILE"
        duration="TIMEOUT"
    else
        echo "❌ FAILED - Exit code: $exit_code" | tee -a "$RESULTS_FILE"
    fi
    
    echo "" | tee -a "$RESULTS_FILE"
    
    # Return duration for table
    echo "$duration"
}

cd /home/djek0/spark

# Track timings for table
declare -A TIMINGS

echo "=== TEST 1: SimpleSparkTest (Honest - Baseline) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T1"]=$(run_test "SimpleSparkTest-Honest" "SimpleSparkTest")

echo "=== TEST 2: ComplexSparkTest (Honest - Default: EXEC=true, MERKLE=true) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T2"]=$(run_test "ComplexSparkTest-Honest-Default" "ComplexSparkTest")

echo "=== TEST 3: ComplexSparkTest (Honest - Driver verification: EXEC=false) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T3"]=$(run_test "ComplexSparkTest-Honest-DriverVerify" "ComplexSparkTest" "EXEC_VERIFICATION=false")

echo "=== TEST 4: ComplexSparkTest (Honest - No Merkle: MERKLE=false) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T4"]=$(run_test "ComplexSparkTest-Honest-NoMerkle" "ComplexSparkTest" "MERKLE_VERIFICATION=false")

echo "=== TEST 5: ComplexSparkTest (Honest - Driver + No Merkle) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T5"]=$(run_test "ComplexSparkTest-Honest-Driver-NoMerkle" "ComplexSparkTest" "EXEC_VERIFICATION=false" "MERKLE_VERIFICATION=false")

echo "=== TEST 6: ComplexSparkTest (Byzantine PROB=1 - All tasks Byzantine) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T6"]=$(run_test "ComplexSparkTest-Byzantine-100%" "ComplexSparkTest" "HONEST=False" "BYZANTINE_PROBABILITY=1")

echo "=== TEST 7: ComplexSparkTest (Byzantine PROB=2 - 50% Byzantine) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T7"]=$(run_test "ComplexSparkTest-Byzantine-50%" "ComplexSparkTest" "HONEST=False" "BYZANTINE_PROBABILITY=2")

echo "=== TEST 8: ComplexSparkTest (Byzantine PROB=3 - 33% Byzantine) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T8"]=$(run_test "ComplexSparkTest-Byzantine-33%" "ComplexSparkTest" "HONEST=False" "BYZANTINE_PROBABILITY=3")

echo "=== TEST 9: ComplexSparkTest (Byzantine + Driver verification) ===" | tee -a "$RESULTS_FILE"
TIMINGS["T9"]=$(run_test "ComplexSparkTest-Byzantine-DriverVerify" "ComplexSparkTest" "HONEST=False" "BYZANTINE_PROBABILITY=2" "EXEC_VERIFICATION=false")

# Generate timing comparison table
echo "" | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "         TIMING COMPARISON TABLE        " | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "Test Configuration" "Time (s)" | tee -a "$RESULTS_FILE"
echo "--------------------------------------------------------------------------------" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T1: SimpleSparkTest (Honest)" "${TIMINGS[T1]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T2: ComplexSparkTest (Honest - Default)" "${TIMINGS[T2]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T3: ComplexSparkTest (Honest - Driver Verify)" "${TIMINGS[T3]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T4: ComplexSparkTest (Honest - No Merkle)" "${TIMINGS[T4]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T5: ComplexSparkTest (Honest - Driver + NoMerkle)" "${TIMINGS[T5]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T6: ComplexSparkTest (Byzantine 100%)" "${TIMINGS[T6]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T7: ComplexSparkTest (Byzantine 50%)" "${TIMINGS[T7]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T8: ComplexSparkTest (Byzantine 33%)" "${TIMINGS[T8]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %10s\n" "T9: ComplexSparkTest (Byzantine + Driver Verify)" "${TIMINGS[T9]}" | tee -a "$RESULTS_FILE"
echo "--------------------------------------------------------------------------------" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"
echo "✅ All tests completed!" | tee -a "$RESULTS_FILE"
echo "📊 Full results saved to: $RESULTS_FILE" | tee -a "$RESULTS_FILE"
