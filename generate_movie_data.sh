#!/bin/bash

# Fast Movie Data Generator
# Generates movies.txt and ratings.txt
# Usage: ./generate_movie_data.sh [movies_file] [ratings_file] [num_movies] [num_ratings]

MOVIES_FILE="${1:-/tmp/movies.txt}"
RATINGS_FILE="${2:-/tmp/ratings.txt}"
NUM_MOVIES="${3:-10000}"
NUM_RATINGS="${4:-5000000}"  # Default 5M ratings

echo "=========================================="
echo "FAST MOVIE DATA GENERATOR"
echo "=========================================="
MOVIES_SIZE_MB=$((NUM_MOVIES * 170 / 1024 / 1024))
RATINGS_SIZE_MB=$((NUM_RATINGS * 20 / 1024 / 1024))
echo "Movies file:       $MOVIES_FILE"
echo "Ratings file:      $RATINGS_FILE"
echo "Number of movies:  $(printf "%'d" $NUM_MOVIES) (~${MOVIES_SIZE_MB}MB)"
echo "Number of ratings: $(printf "%'d" $NUM_RATINGS) (~${RATINGS_SIZE_MB}MB)"
echo "=========================================="
echo ""

START_TIME=$(date +%s)

mkdir -p "$(dirname "$MOVIES_FILE")"
mkdir -p "$(dirname "$RATINGS_FILE")"

# Generate Movies
echo "[1/3] Generating movies data..."

ADJECTIVES=("Dark" "Silent" "Lost" "Hidden" "Forgotten" "Secret" "Last" "First" "Eternal" "Infinite" "Final" "Ultimate" "Perfect" "Broken" "Ancient" "Modern" "Wild" "Dangerous" "Beautiful" "Mysterious")
NOUNS=("Night" "Dawn" "Storm" "Shadow" "Light" "Dream" "Legend" "Hero" "Warrior" "Kingdom" "Empire" "City" "World" "Journey" "Quest" "Battle" "War" "Peace" "Love" "Death" "Dragon" "Phoenix" "Wolf" "Tiger")
SUFFIXES=("" " Returns" " Rises" " Falls" " Begins" " Ends" ": Part 1" ": Part 2")
LANGUAGES=("en" "en" "en" "en" "en" "es" "fr" "de" "it" "pt" "ja" "ko" "zh" "ru")

export ADJ_STR="${ADJECTIVES[*]}"
export NOUNS_STR="${NOUNS[*]}"
export SUFF_STR="${SUFFIXES[*]}"
export LANG_STR="${LANGUAGES[*]}"

awk -v num_movies=$NUM_MOVIES \
    -v adj="$ADJ_STR" \
    -v nouns="$NOUNS_STR" \
    -v suff="$SUFF_STR" \
    -v lang="$LANG_STR" '
BEGIN {
    split(adj, adj_arr, " ")
    split(nouns, noun_arr, " ")
    split(suff, suff_arr, " ")
    split(lang, lang_arr, " ")
    
    for (i = 1; i <= num_movies; i++) {
        imdb_num = (i * 2654435761) % 10000000
        imdb_id = sprintf("tt%07d", imdb_num)
        language = lang_arr[1 + (i * 1103515245) % length(lang_arr)]
        
        adj_word = adj_arr[1 + (i * 48271) % length(adj_arr)]
        noun_word = noun_arr[1 + (i * 69621) % length(noun_arr)]
        suffix = suff_arr[1 + (i * 40692) % length(suff_arr)]
        
        title = adj_word " " noun_word suffix
        
        printf "%d,%s,%s,%s\n", i, imdb_id, language, title
        
        if (i % 1000 == 0) {
            printf "  Progress: %d/%d (%.1f%%)...\r", i, num_movies, (i*100.0/num_movies) > "/dev/stderr"
        }
    }
    print "" > "/dev/stderr"
}' > "$MOVIES_FILE"

echo "[2/3] Generating ratings data..."

NUM_USERS=100000

awk -v num_ratings=$NUM_RATINGS \
    -v num_movies=$NUM_MOVIES \
    -v num_users=$NUM_USERS '
BEGIN {
    base_timestamp = 1262304000  # 2010-01-01
    time_range = 14 * 365 * 24 * 3600  # 14 years
    
    for (i = 1; i <= num_ratings; i++) {
        user_id = 1 + (i * 2654435761) % num_users
        
        # Some movies more popular
        movie_seed = i * 1103515245 + 12345
        movie_id = 1 + ((movie_seed % (num_movies * num_movies)) / num_movies) % num_movies
        
        rating_val = 1.0 + ((i * 48271) % 10) / 2.0
        if (rating_val > 5.0) rating_val = 5.0
        
        timestamp = base_timestamp + (i * 69621) % time_range
        
        printf "%d,%d,%.1f,%d\n", user_id, movie_id, rating_val, timestamp
        
        if (i % 500000 == 0) {
            printf "  Progress: %d/%d (%.1f%%)...\r", i, num_ratings, (i*100.0/num_ratings) > "/dev/stderr"
        }
    }
    print "" > "/dev/stderr"
}' > "$RATINGS_FILE"

echo "[3/3] Data generation complete!"
echo ""

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

MOVIES_SIZE=$(du -h "$MOVIES_FILE" | cut -f1)
RATINGS_SIZE=$(du -h "$RATINGS_FILE" | cut -f1)

echo "=========================================="
echo "GENERATION COMPLETE"
echo "=========================================="
echo "Movies saved to:   $MOVIES_FILE ($MOVIES_SIZE)"
echo "Ratings saved to:  $RATINGS_FILE ($RATINGS_SIZE)"
echo "Time taken:        ${DURATION}s"
echo "=========================================="
echo ""
echo "To use with MovieReviewAnalysis:"
echo "  ./bin/run-example MovieReviewAnalysis $MOVIES_FILE $RATINGS_FILE"
