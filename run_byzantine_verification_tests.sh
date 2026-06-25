#!/bin/bash

# Byzantine Verification Test Suite
# Tests all combinations of verification strategies with BYZANTINE_PROBABILITY=3 (33% Byzantine)
#
# Usage: ./run_byzantine_verification_tests.sh [ExampleName]
# Example: ./run_byzantine_verification_tests.sh MerkleVsFullTaskComparison
# Default: ComplexSparkTest

EXAMPLE_NAME="${1:-ComplexSparkTest}"

RESULTS_FILE="/tmp/spark_byzantine_tests.txt"
> "$RESULTS_FILE"  # Clear file

# Calculate adaptive timeout based on test complexity
get_test_timeout() {
    case "$EXAMPLE_NAME" in
        # Heavy I/O + shuffle + groupBy operations
        TestCollapsers|TestResultTaskLogging)
            echo 300  # 5 minutes - coalesce, repartition, groupBy are expensive
            ;;
        # Nondeterministic operations + shuffle (multiple retries expected)
        TestReorders|TestDroppers)
            echo 240  # 4 minutes - sorting, distinct, sample with shuffles
            ;;
        # File logging overhead + multiple stages
        TestSafeTransformations)
            echo 180  # 3 minutes - mapPartitions with file I/O
            ;;
        # Sampling operations (lightweight but many stages)
        TestSamples)
            echo 150  # 2.5 minutes - sample operations
            ;;
        # Default for simple/typical tests
        *)
            echo 120  # 2 minutes - baseline
            ;;
    esac
}

TIMEOUT_SECONDS=$(get_test_timeout)

echo "========================================" | tee -a "$RESULTS_FILE"
echo "   BYZANTINE VERIFICATION TEST SUITE    " | tee -a "$RESULTS_FILE"
echo "  (33% Byzantine - PROBABILITY=3)       " | tee -a "$RESULTS_FILE"
echo "  Example: $EXAMPLE_NAME                " | tee -a "$RESULTS_FILE"
echo "  Timeout: ${TIMEOUT_SECONDS}s          " | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

run_test() {
    local test_num=$1
    local test_name=$2
    shift 2
    local env_vars="$@"
    
    echo "=======================================" | tee -a "$RESULTS_FILE"
    echo "TEST $test_num: $test_name" | tee -a "$RESULTS_FILE"
    echo "ENV: $env_vars" | tee -a "$RESULTS_FILE"
    echo "=======================================" | tee -a "$RESULTS_FILE"
    
    # Run and time the test
    start_time=$(date +%s.%N)
    
    if [ -z "$env_vars" ]; then
        timeout ${TIMEOUT_SECONDS}s ./bin/run-example "$EXAMPLE_NAME" 2>&1 | grep -E "CONSENSUS|VERIFICATION|BYZANTINE|Final Output|Group.*sum|Verdict|REPLICA.*CORRECT|NEITHER_MATCH" | tail -30 | tee -a "$RESULTS_FILE"
        exit_code=${PIPESTATUS[0]}
    else
        timeout ${TIMEOUT_SECONDS}s env $env_vars ./bin/run-example "$EXAMPLE_NAME" 2>&1 | grep -E "CONSENSUS|VERIFICATION|BYZANTINE|Final Output|Group.*sum|Verdict|REPLICA.*CORRECT|NEITHER_MATCH" | tail -30 | tee -a "$RESULTS_FILE"
        exit_code=${PIPESTATUS[0]}
    fi
    
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    
    if [ $exit_code -eq 0 ]; then
        echo "[PASS] PASSED - Duration: ${duration}s" | tee -a "$RESULTS_FILE"
        status="PASS"
    elif [ $exit_code -eq 124 ]; then
        echo "[TIMEOUT] Test timed out after ${TIMEOUT_SECONDS}s" | tee -a "$RESULTS_FILE"
        duration="TIMEOUT"
        status="TIMEOUT"
    else
        echo "[FAIL] Exit code: $exit_code - Duration: ${duration}s" | tee -a "$RESULTS_FILE"
        status="FAIL"
    fi
    
    echo "" | tee -a "$RESULTS_FILE"
    
    # Return duration and status for table
    echo "$duration|$status"
}

cd /home/djek0/spark

# Track results for summary table
declare -A TIMINGS
declare -A STATUSES

echo "" | tee -a "$RESULTS_FILE"
echo "=== TEST 1: Baseline - All Honest (No Byzantine behavior) ===" | tee -a "$RESULTS_FILE"
result=$(run_test "1" "All Honest - Baseline")
TIMINGS["T1"]=$(echo "$result" | cut -d'|' -f1)
STATUSES["T1"]=$(echo "$result" | cut -d'|' -f2)

echo "" | tee -a "$RESULTS_FILE"
echo "=== TEST 2: Byzantine - Driver Full Task Recomputation ===" | tee -a "$RESULTS_FILE"
echo "Config: EXEC_VERIFICATION=false, MERKLE_VERIFICATION=false" | tee -a "$RESULTS_FILE"
result=$(run_test "2" "Driver Full Task" "HONEST=False" "BYZANTINE_PROBABILITY=10" "EXEC_VERIFICATION=false" "MERKLE_VERIFICATION=false")
TIMINGS["T2"]=$(echo "$result" | cut -d'|' -f1)
STATUSES["T2"]=$(echo "$result" | cut -d'|' -f2)

echo "" | tee -a "$RESULTS_FILE"
echo "=== TEST 3: Byzantine - Driver Merkle (Trees on Driver) ===" | tee -a "$RESULTS_FILE"
echo "Config: EXEC_VERIFICATION=false, MERKLE_VERIFICATION=true, MERKLE_BUILD_ON_DRIVER=true" | tee -a "$RESULTS_FILE"
result=$(run_test "3" "Driver Merkle (Trees on Driver)" "HONEST=False" "BYZANTINE_PROBABILITY=10" "EXEC_VERIFICATION=false" "MERKLE_VERIFICATION=true" "MERKLE_BUILD_ON_DRIVER=true")
TIMINGS["T3"]=$(echo "$result" | cut -d'|' -f1)
STATUSES["T3"]=$(echo "$result" | cut -d'|' -f2)

echo "" | tee -a "$RESULTS_FILE"
echo "=== TEST 4: Byzantine - Driver Merkle (Trees on Executors) ===" | tee -a "$RESULTS_FILE"
echo "Config: EXEC_VERIFICATION=false, MERKLE_VERIFICATION=true, MERKLE_BUILD_ON_DRIVER=false" | tee -a "$RESULTS_FILE"
result=$(run_test "4" "Driver Merkle (Trees on Executors)" "HONEST=False" "BYZANTINE_PROBABILITY=10 " "EXEC_VERIFICATION=false" "MERKLE_VERIFICATION=true" "MERKLE_BUILD_ON_DRIVER=false")
TIMINGS["T4"]=$(echo "$result" | cut -d'|' -f1)
STATUSES["T4"]=$(echo "$result" | cut -d'|' -f2)

echo "" | tee -a "$RESULTS_FILE"
echo "=== TEST 5: Byzantine - 3rd Executor Full Task Recomputation ===" | tee -a "$RESULTS_FILE"
echo "Config: EXEC_VERIFICATION=true, MERKLE_VERIFICATION=false" | tee -a "$RESULTS_FILE"
result=$(run_test "5" "3rd Executor Full Task" "HONEST=False" "BYZANTINE_PROBABILITY=10" "EXEC_VERIFICATION=true" "MERKLE_VERIFICATION=false")
TIMINGS["T5"]=$(echo "$result" | cut -d'|' -f1)
STATUSES["T5"]=$(echo "$result" | cut -d'|' -f2)

echo "" | tee -a "$RESULTS_FILE"
echo "=== TEST 6: Byzantine - 3rd Executor Merkle (Trees on Driver) ===" | tee -a "$RESULTS_FILE"
echo "Config: EXEC_VERIFICATION=true, MERKLE_VERIFICATION=true, MERKLE_BUILD_ON_DRIVER=true" | tee -a "$RESULTS_FILE"
result=$(run_test "6" "3rd Executor Merkle (Trees on Driver)" "HONEST=False" "BYZANTINE_PROBABILITY=10" "EXEC_VERIFICATION=true" "MERKLE_VERIFICATION=true" "MERKLE_BUILD_ON_DRIVER=true")
TIMINGS["T6"]=$(echo "$result" | cut -d'|' -f1)
STATUSES["T6"]=$(echo "$result" | cut -d'|' -f2)

echo "" | tee -a "$RESULTS_FILE"
echo "=== TEST 7: Byzantine - 3rd Executor Merkle (Trees on Executors) ===" | tee -a "$RESULTS_FILE"
echo "Config: EXEC_VERIFICATION=true, MERKLE_VERIFICATION=true, MERKLE_BUILD_ON_DRIVER=false" | tee -a "$RESULTS_FILE"
result=$(run_test "7" "3rd Executor Merkle (Trees on Executors)" "HONEST=False" "BYZANTINE_PROBABILITY=10" "EXEC_VERIFICATION=true" "MERKLE_VERIFICATION=true" "MERKLE_BUILD_ON_DRIVER=false")
TIMINGS["T7"]=$(echo "$result" | cut -d'|' -f1)
STATUSES["T7"]=$(echo "$result" | cut -d'|' -f2)

# Generate summary table
echo "" | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "           RESULTS SUMMARY              " | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

printf "%-50s | %-10s | %10s\n" "Test Configuration" "Status" "Time (s)" | tee -a "$RESULTS_FILE"
echo "--------------------------------------------------------------------------------" | tee -a "$RESULTS_FILE"
printf "%-50s | %-10s | %10s\n" "T1: All Honest (Baseline)" "${STATUSES[T1]}" "${TIMINGS[T1]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %-10s | %10s\n" "T2: Driver Full Task" "${STATUSES[T2]}" "${TIMINGS[T2]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %-10s | %10s\n" "T3: Driver Merkle (Trees on Driver)" "${STATUSES[T3]}" "${TIMINGS[T3]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %-10s | %10s\n" "T4: Driver Merkle (Trees on Executors)" "${STATUSES[T4]}" "${TIMINGS[T4]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %-10s | %10s\n" "T5: 3rd Executor Full Task" "${STATUSES[T5]}" "${TIMINGS[T5]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %-10s | %10s\n" "T6: 3rd Executor Merkle (Trees on Driver)" "${STATUSES[T6]}" "${TIMINGS[T6]}" | tee -a "$RESULTS_FILE"
printf "%-50s | %-10s | %10s\n" "T7: 3rd Executor Merkle (Trees on Executors)" "${STATUSES[T7]}" "${TIMINGS[T7]}" | tee -a "$RESULTS_FILE"
echo "--------------------------------------------------------------------------------" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Count results
PASS_COUNT=$(echo "${STATUSES[@]}" | tr ' ' '\n' | grep -c "PASS")
FAIL_COUNT=$(echo "${STATUSES[@]}" | tr ' ' '\n' | grep -c "FAIL")
TIMEOUT_COUNT=$(echo "${STATUSES[@]}" | tr ' ' '\n' | grep -c "TIMEOUT")

echo "Summary: $PASS_COUNT PASSED, $FAIL_COUNT FAILED, $TIMEOUT_COUNT TIMEOUT" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"
echo "[DONE] All tests completed!" | tee -a "$RESULTS_FILE"
echo "[INFO] Full results saved to: $RESULTS_FILE" | tee -a "$RESULTS_FILE"

# Show results file location
cat "$RESULTS_FILE"
