#!/bin/bash
#
# Performance Test Suite for Byzantine Fault Tolerance
# Tests different verification strategies and captures execution times
#

set -e

RESULTS_FILE="performance_results.txt"
RESULTS_CSV="performance_results.csv"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "========================================"
echo "Byzantine Fault Tolerance Performance Tests"
echo "Started: $(date)"
echo "========================================"
echo ""

# Initialize results file
cat > $RESULTS_FILE << EOF
Byzantine Fault Tolerance - Performance Test Results
Generated: $(date)
====================================================

EOF

# Initialize CSV
echo "Test,Description,HONEST,EXEC_VERIFICATION,MERKLE_VERIFICATION,Duration_Seconds,Status" > $RESULTS_CSV

# Function to update docker-compose.yml
update_config() {
    local honest=$1
    local exec_ver=$2
    local merkle_ver=$3
    
    echo -e "${YELLOW}Updating configuration...${NC}"
    
    # Update HONEST for worker2
    sed -i "s/- HONEST=.*/- HONEST=$honest/" docker-compose.yml
    
    # Update EXEC_VERIFICATION for master
    sed -i "s/- EXEC_VERIFICATION=.*/- EXEC_VERIFICATION=$exec_ver/" docker-compose.yml
    
    # Update MERKLE_VERIFICATION for master
    sed -i "s/- MERKLE_VERIFICATION=.*/- MERKLE_VERIFICATION=$merkle_ver/" docker-compose.yml
}

# Function to run a single test
run_test() {
    local test_num=$1
    local desc=$2
    local honest=$3
    local exec_ver=$4
    local merkle_ver=$5
    
    echo ""
    echo "========================================"
    echo -e "${GREEN}Test $test_num: $desc${NC}"
    echo "  HONEST=$honest"
    echo "  EXEC_VERIFICATION=$exec_ver"
    echo "  MERKLE_VERIFICATION=$merkle_ver"
    echo "========================================"
    
    # Update configuration
    update_config "$honest" "$exec_ver" "$merkle_ver"
    
    # Restart cluster
    echo -e "${YELLOW}Restarting cluster...${NC}"
    docker compose down > /dev/null 2>&1
    docker compose up -d > /dev/null 2>&1
    
    # Wait for cluster to be ready
    echo "Waiting for cluster startup..."
    sleep 15
    
    # Run test and capture output
    echo -e "${YELLOW}Running test...${NC}"
    LOG_FILE="test_${test_num}.log"
    
    START_TIME=$(date +%s.%N)
    
    if docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://master:7077 \
        --class org.apache.spark.examples.SimpleSparkTest \
        /apps/spark-examples_2.12-3.3.0-SNAPSHOT.jar \
        > "$LOG_FILE" 2>&1; then
        STATUS="SUCCESS"
    else
        STATUS="FAILED"
    fi
    
    END_TIME=$(date +%s.%N)
    DURATION=$(echo "$END_TIME - $START_TIME" | bc)
    
    # Extract job duration from Spark logs
    SPARK_DURATION=$(grep "Job 0 finished:" "$LOG_FILE" | sed -n 's/.*took \([0-9.]*\) s/\1/p')
    
    if [ -z "$SPARK_DURATION" ]; then
        SPARK_DURATION="N/A"
        echo -e "${RED}Warning: Could not extract Spark job duration${NC}"
    fi
    
    # Display results
    echo -e "${GREEN}Test completed!${NC}"
    echo "  Total execution time: ${DURATION}s"
    echo "  Spark job time: ${SPARK_DURATION}s"
    echo "  Status: $STATUS"
    
    # Write to results file
    cat >> $RESULTS_FILE << EOF
Test $test_num: $desc
  Configuration:
    HONEST=$honest
    EXEC_VERIFICATION=$exec_ver
    MERKLE_VERIFICATION=$merkle_ver
  Results:
    Total execution time: ${DURATION}s
    Spark job time: ${SPARK_DURATION}s
    Status: $STATUS
    Log file: $LOG_FILE

EOF
    
    # Write to CSV
    echo "$test_num,\"$desc\",$honest,$exec_ver,$merkle_ver,$SPARK_DURATION,$STATUS" >> $RESULTS_CSV
}

# Test 1: Baseline - All Honest
run_test 1 "Baseline: All Honest (No Verification)" "true" "false" "false"

# Test 2: Driver Full-Task Recompute
run_test 2 "Driver Full-Task Verification" "False" "false" "false"

# Test 3: Driver Merkle Verification
run_test 3 "Driver Merkle-Based Verification" "False" "false" "true"

# Test 4: 3rd Executor Full-Task
run_test 4 "3rd Executor Full-Task Verification" "False" "true" "false"

# Test 5: 3rd Executor Merkle
run_test 5 "3rd Executor Merkle Verification" "False" "true" "true"

# Generate summary table
echo ""
echo "========================================"
echo -e "${GREEN}All tests completed!${NC}"
echo "========================================"
echo ""
echo "Results Summary:"
echo "----------------"

# Display CSV in table format
column -t -s',' $RESULTS_CSV

echo ""
echo "Detailed results saved to: $RESULTS_FILE"
echo "CSV results saved to: $RESULTS_CSV"
echo "Individual logs: test_1.log through test_5.log"
echo ""
echo "Finished: $(date)"
