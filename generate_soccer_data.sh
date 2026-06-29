#!/bin/bash

# Fast Soccer Data Generator
# Usage: ./generate_soccer_data.sh [output_file] [num_players]

OUTPUT_FILE="${1:-/tmp/soccer_data.txt}"
NUM_PLAYERS="${2:-800000}"  # Default 800K players

echo "=========================================="
echo "FAST SOCCER DATA GENERATOR"
echo "=========================================="
SIZE_MB=$((NUM_PLAYERS * 25 / 1024 / 1024))
echo "Output file:       $OUTPUT_FILE"
echo "Number of players: $(printf "%'d" $NUM_PLAYERS) (~${SIZE_MB}MB)"
echo "=========================================="
echo ""

START_TIME=$(date +%s)

mkdir -p "$(dirname "$OUTPUT_FILE")"

echo "[1/2] Generating data..."

# Arrays for realistic data
COUNTRIES=("Brazil" "Argentina" "France" "Spain" "Germany" "Italy" "England" "Netherlands" "Portugal" "Belgium" "Croatia" "Uruguay" "Colombia" "Mexico" "Poland" "Denmark" "Switzerland" "Sweden" "Austria" "Czech Republic" "Turkey" "Ukraine" "Serbia" "Russia" "Greece" "Romania" "Norway" "Japan" "South Korea" "USA" "Canada" "Australia" "Nigeria" "Ghana" "Ivory Coast" "Senegal" "Egypt" "Morocco" "Tunisia" "Algeria" "Chile" "Peru" "Ecuador" "Paraguay" "Costa Rica" "Panama" "Honduras" "Jamaica" "China" "Iran")

POSITIONS=("GK" "CB" "LB" "RB" "CDM" "CM" "CAM" "LW" "RW" "ST")

FIRST_NAMES=("Marco" "Luca" "Diego" "Carlos" "Juan" "Luis" "David" "Miguel" "Antonio" "José" "Manuel" "Francisco" "Rafael" "Daniel" "Gabriel" "Pedro" "Fernando" "Javier" "Roberto" "Alejandro" "Sergio" "Jorge" "Ricardo" "Alberto" "Eduardo" "Felipe" "Andrés" "Pablo" "Cristiano" "Lionel")

LAST_NAMES=("Silva" "Santos" "Oliveira" "Pereira" "Costa" "Rodrigues" "Martins" "Lopez" "Martinez" "Garcia" "Rodriguez" "Hernandez" "Gonzalez" "Perez" "Sanchez" "Ramirez" "Torres" "Flores" "Rivera" "Gomez" "Diaz" "Cruz" "Morales" "Reyes" "Müller" "Schmidt" "Fischer" "Weber" "Meyer" "Wagner")

CLUBS=("Real Madrid" "Barcelona" "Bayern Munich" "Manchester City" "Liverpool" "PSG" "Juventus" "Chelsea" "Manchester United" "Arsenal" "Inter Milan" "AC Milan" "Atletico Madrid" "Dortmund" "Napoli" "Tottenham" "Roma" "Ajax" "Benfica" "Porto")

# Export arrays as strings
export COUNTRIES_STR="${COUNTRIES[*]}"
export POSITIONS_STR="${POSITIONS[*]}"
export FIRST_NAMES_STR="${FIRST_NAMES[*]}"
export LAST_NAMES_STR="${LAST_NAMES[*]}"
export CLUBS_STR="${CLUBS[*]}"

awk -v num_players=$NUM_PLAYERS \
    -v countries="$COUNTRIES_STR" \
    -v positions="$POSITIONS_STR" \
    -v first_names="$FIRST_NAMES_STR" \
    -v last_names="$LAST_NAMES_STR" \
    -v clubs="$CLUBS_STR" '
BEGIN {
    split(countries, c_arr, " ")
    split(positions, p_arr, " ")
    split(first_names, fn_arr, " ")
    split(last_names, ln_arr, " ")
    split(clubs, cl_arr, " ")
    
    srand()
    for (i = 1; i <= num_players; i++) {
        country = c_arr[1 + (i * 2654435761) % length(c_arr)]
        position = p_arr[1 + (i * 1103515245) % length(p_arr)]
        first_name = fn_arr[1 + (i * 48271) % length(fn_arr)]
        last_name = ln_arr[1 + (i * 69621) % length(ln_arr)]
        age = 18 + (i * 40692) % 20
        club = cl_arr[1 + (i * 25214) % length(cl_arr)]
        
        printf "%d,%s %s,%s,%s,%d,%s\n", i, first_name, last_name, country, position, age, club
        
        if (i % 100000 == 0) {
            printf "  Progress: %d/%d (%.1f%%)...\r", i, num_players, (i*100.0/num_players) > "/dev/stderr"
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
echo "To use with SoccerPlayerAnalysis:"
echo "  ./bin/run-example SoccerPlayerAnalysis $OUTPUT_FILE"
