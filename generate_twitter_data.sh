#!/bin/bash

# Fast Twitter Data Generator
# Generates follower-followee relationships without Spark overhead
# Usage: ./generate_twitter_data.sh [output_file] [num_edges]

OUTPUT_FILE="${1:-/tmp/twitter_data.txt}"
NUM_EDGES="${2:-5000000}"  # Default 5M edges
NUM_USERS=1000000

echo "=========================================="
echo "FAST TWITTER DATA GENERATOR"
echo "=========================================="
SIZE_MB=$((NUM_EDGES * 25 / 1024 / 1024))
echo "Output file:    $OUTPUT_FILE"
echo "Number of edges: $(printf "%'d" $NUM_EDGES) (~${SIZE_MB}MB)"
echo "Number of users: $(printf "%'d" $NUM_USERS)"
echo "=========================================="
echo ""

START_TIME=$(date +%s)

# Create output directory if needed
mkdir -p "$(dirname "$OUTPUT_FILE")"

echo "[1/2] Generating data..."

# Use awk for fast generation (much faster than Spark)
awk -v num_edges=$NUM_EDGES -v num_users=$NUM_USERS '
BEGIN {
    srand()
    for (i = 1; i <= num_edges; i++) {
        # Simple hash-based pseudo-random generation
        follower = (i * 2654435761) % num_users
        followee = (i * 1103515245 + 12345) % num_users
        
        # Avoid self-follows
        if (follower == followee) {
            followee = (follower + 1) % num_users
        }
        
        print "user_" follower " user_" followee
        
        # Progress indicator
        if (i % 1000000 == 0) {
            printf "  Progress: %d/%d (%.1f%%)...\r", i, num_edges, (i*100.0/num_edges) > "/dev/stderr"
        }
    }
    print "" > "/dev/stderr"
}' > "$OUTPUT_FILE"

echo "[2/2] Data generation complete!"
echo ""

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)

echo "=========================================="
echo "GENERATION COMPLETE"
echo "=========================================="
echo "File saved to:  $OUTPUT_FILE"
echo "File size:      $FILE_SIZE"
echo "Time taken:     ${DURATION}s"
echo "=========================================="
echo ""
echo "To use with TwitterFollowerAnalysis:"
echo "  ./bin/run-example TwitterFollowerAnalysis $OUTPUT_FILE"
