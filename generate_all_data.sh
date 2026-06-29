#!/bin/bash

# Generate all datasets for the three applications
# This script generates smaller, faster datasets suitable for testing

echo "=========================================="
echo "GENERATING ALL TEST DATASETS"
echo "=========================================="
echo ""

# Make scripts executable
chmod +x generate_twitter_data.sh generate_soccer_data.sh generate_movie_data.sh

# Generate Twitter data (5M edges = ~130MB)
echo "=== APPLICATION 1: Twitter Data ==="
./generate_twitter_data.sh /tmp/twitter_data.txt 5000000
echo ""

# Generate Soccer data (800K players = ~20MB)
echo "=== APPLICATION 2: Soccer Data ==="
./generate_soccer_data.sh /tmp/soccer_data.txt 800000
echo ""

# Generate Movie data (10K movies + 5M ratings)
echo "=== APPLICATION 3: Movie Data ==="
./generate_movie_data.sh /tmp/movies.txt /tmp/ratings.txt 10000 5000000
echo ""

echo "=========================================="
echo "ALL DATASETS GENERATED"
echo "=========================================="
echo ""
echo "Generated files:"
echo "  /tmp/twitter_data.txt  (Application 1)"
echo "  /tmp/soccer_data.txt   (Application 2)"
echo "  /tmp/movies.txt        (Application 3)"
echo "  /tmp/ratings.txt       (Application 3)"
echo ""
echo "To run the applications:"
echo "  ./bin/run-example TwitterFollowerAnalysis 16 /tmp/twitter_data.txt"
echo "  ./bin/run-example SoccerPlayerAnalysis 16 /tmp/soccer_data.txt"
echo "  ./bin/run-example MovieReviewAnalysis 16 /tmp/movies.txt /tmp/ratings.txt"
echo ""
