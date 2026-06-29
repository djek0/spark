# Data Sources for Byzantine Verification Examples

## Quick Generation (RECOMMENDED for testing)

**Generate all datasets in ~30 seconds:**
```bash
chmod +x generate_all_data.sh
./generate_all_data.sh
```

This creates smaller datasets perfect for testing:
- Twitter: 5M edges (~130MB) - faster than 50M
- Soccer: 800K players (~20MB)
- Movies: 10K movies + 5M ratings (~1.7MB + 100MB)

---

## Real Dataset Sources (Optional)

### Application 1: Twitter Social Graph

**Option 1: Twitter Social Graph (Small)**
- **Source**: Stanford SNAP
- **URL**: https://snap.stanford.edu/data/ego-Twitter.html
- **Size**: 1.4GB (uncompressed)
- **Format**: `follower followee` (space-separated)
- **Download**:
  ```bash
  wget https://snap.stanford.edu/data/twitter_combined.txt.gz
  gunzip twitter_combined.txt.gz
  mv twitter_combined.txt /tmp/twitter_data.txt
  ```

**Option 2: Twitter Graph (Large)**
- **Source**: Stanford SNAP
- **URL**: https://snap.stanford.edu/data/twitter-2010.html
- **Size**: 23GB (uncompressed)
- **Note**: Very large, use only for serious performance testing

---

### Application 2: Soccer Players

**Option 1: Generated Data (RECOMMENDED)**
- Use our fast generator: `./generate_soccer_data.sh /tmp/soccer_data.txt 800`
- Realistic, deterministic, and fast

**Option 2: FIFA Players Dataset**
- **Source**: Kaggle
- **URL**: https://www.kaggle.com/stefanoleone992/fifa-22-complete-player-dataset
- **Size**: ~20MB
- **Format**: CSV with player stats
- **Note**: Requires Kaggle account and format conversion

---

### Application 3: Movie Reviews

**Option 1: MovieLens Small**
- **Source**: GroupLens Research
- **URL**: https://grouplens.org/datasets/movielens/
- **Size**: ~1MB movies + 24MB ratings
- **Format**: CSV
- **Download**:
  ```bash
  wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
  unzip ml-25m.zip
  # Extract relevant files
  ```

**Option 2: Generated Data (RECOMMENDED)**
- Use our fast generator: `./generate_movie_data.sh /tmp/movies.txt /tmp/ratings.txt 10000 5`
- Perfect format match, fast generation

---

## Dataset Size Comparison

| Dataset | Synthetic (Fast) | Real (Optional) | Generation Time |
|---------|------------------|-----------------|-----------------|
| **Twitter** | 130MB (5M edges) | 1.4GB (41M edges) | ~10 seconds |
| **Soccer** | 20MB (800K players) | ~20MB (varies) | ~5 seconds |
| **Movies** | 2MB + 100MB | 1MB + 24MB | ~15 seconds |
| **TOTAL** | ~232MB | ~1.4GB+ | **~30 seconds** |

---

## Usage Examples

### Fast Generation (Recommended)
```bash
# Generate all at once
./generate_all_data.sh

# Or generate individually with custom sizes
./generate_twitter_data.sh /tmp/twitter_data.txt 10  # 10M edges
./generate_soccer_data.sh /tmp/soccer_data.txt 500   # 500K players
./generate_movie_data.sh /tmp/movies.txt /tmp/ratings.txt 20000 10  # 20K movies, 10M ratings
```

### Run Applications
```bash
# Application 1: Twitter
./bin/run-example TwitterFollowerAnalysis 16 /tmp/twitter_data.txt

# Application 2: Soccer
./bin/run-example SoccerPlayerAnalysis 16 /tmp/soccer_data.txt

# Application 3: Movies
./bin/run-example MovieReviewAnalysis 16 /tmp/movies.txt /tmp/ratings.txt
```

### With Byzantine Verification
```bash
HONEST=False BYZANTINE_PROBABILITY=10 EXEC_VERIFICATION=true MERKLE_VERIFICATION=true \
  ./bin/run-example TwitterFollowerAnalysis 32 /tmp/twitter_data.txt
```

---

## Performance Tips

1. **Start small**: Use generated data (5M edges for Twitter)
2. **Scale up gradually**: Increase sizes once everything works
3. **Real data**: Only use for final benchmarks
4. **Local mode**: Sufficient for testing Byzantine verification
5. **Partitions**: Match to your CPU cores (8-32 typical)

---

## Troubleshooting

**Generation too slow?**
- Reduce dataset size (e.g., 1M edges instead of 5M)
- Use `/dev/shm` for faster I/O: `./generate_twitter_data.sh /dev/shm/twitter_data.txt 1`

**Out of memory?**
- Reduce partition count
- Increase `spark.executor.memory` in conf/spark-defaults.conf
- Use smaller datasets

**Want exact sizes from paper?**
- Twitter: `./generate_twitter_data.sh /tmp/twitter_data.txt 50` (1.3GB)
- But expect ~20-30 minutes generation time
