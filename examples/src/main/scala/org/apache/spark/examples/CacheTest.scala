package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Cache Test - Tests cache detection for UID tracking
 *
 * This application tests the cache detection mechanism that prevents
 * UID correlation errors when cached data is reused. When cache is hit,
 * compute() is not called, so UIDs are not enqueued. The cache detection
 * flag triggers UNSAFE_STAGE sentinel to avoid dequeue errors.
 *
 * Usage:
 *   ./bin/run-example CacheTest [partitions] [movies_file] [ratings_file]
 *
 * Parameters:
 *   partitions:    Number of partitions for Spark operations (default: auto-detect)
 *   movies_file:   Path to movies data file (required)
 *   ratings_file:  Path to ratings data file (required)
 */
object CacheTest {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val partitions = if (args.length > 0 && args(0).forall(_.isDigit)) {
      args(0).toInt
    } else {
      -1  // Auto-detect
    }

    val moviesFile = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 1) Some(args(1)) else None
    } else {
      if (args.length > 0) Some(args(0)) else None
    }

    val ratingsFile = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 2) Some(args(2)) else None
    } else {
      if (args.length > 1) Some(args(1)) else None
    }

    val conf = new SparkConf()
      .setAppName("Cache Test")
      .setMaster("local[*]")

    // Set default parallelism if specified
    if (partitions > 0) {
      conf.set("spark.default.parallelism", partitions.toString)
    }

    val sc = new SparkContext(conf)

    try {
      val startTime = System.currentTimeMillis()

      // Validate input
      if (moviesFile.isEmpty || ratingsFile.isEmpty) {
        println("ERROR: Both movies file and ratings file are required!")
        println("")
        println("Usage: ./bin/run-example CacheTest [partitions] [movies_file] [ratings_file]")
        println("")
        System.exit(1)
      }

      // Get Spark configuration info
      val defaultParallelism = sc.defaultParallelism
      val masterUrl = sc.master
      val numExecutors = sc.getExecutorMemoryStatus.size - 1  // Subtract driver

      println("=" * 80)
      println("CACHE TEST - Testing Cache Detection for UID Tracking")
      println("=" * 80)
      println(s"Spark Master:         $masterUrl")
      println(s"Default Parallelism:  $defaultParallelism partitions")
      println(s"Executors:            $numExecutors")
      println(s"Movies File:          ${moviesFile.get}")
      println(s"Ratings File:         ${ratingsFile.get}")
      println("=" * 80)
      println()

      // PHASE 1: Load Movies
      println(s"[PHASE 1] Loading movies from: ${moviesFile.get}")
      val minPartitions = if (partitions > 0) partitions else 16

      val movies = sc.textFile(moviesFile.get, minPartitions = minPartitions)
        .map { line =>
          val parts = line.split(",", 4)  // Split into max 4 parts (title may contain commas)
          if (parts.length >= 4) {
            val movieId = parts(0).trim.toInt
            val imdbId = parts(1).trim
            val language = parts(2).trim
            val title = parts(3).trim
            (movieId, (imdbId, language, title))
          } else {
            (-1, ("", "", ""))
          }
        }
        .filter { case (id, _) => id != -1 }

      val moviesPartitions = movies.getNumPartitions
      val totalMovies = movies.count()

      val phase1Time = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"  Movies loaded: ${"%,d".format(totalMovies)} (in $moviesPartitions partitions)")
      println(s"  Load time: ${phase1Time}s")
      println()

      // PHASE 2: Load Ratings
      println(s"[PHASE 2] Loading ratings from: ${ratingsFile.get}")
      val phase2Start = System.currentTimeMillis()

      val ratings = sc.textFile(ratingsFile.get, minPartitions = minPartitions)
        .map { line =>
          val parts = line.split(",")
          if (parts.length >= 4) {
            val userId = parts(0).trim.toInt
            val movieId = parts(1).trim.toInt
            val rating = parts(2).trim.toDouble
            val timestamp = parts(3).trim.toLong
            (movieId, (userId, rating, timestamp))
          } else {
            (-1, (-1, 0.0, 0L))
          }
        }
        .filter { case (id, _) => id != -1 }

      val ratingsPartitions = ratings.getNumPartitions
      val totalRatings = ratings.count()

      val phase2Time = (System.currentTimeMillis() - phase2Start) / 1000.0
      println(s"  Ratings loaded: ${"%,d".format(totalRatings)} (in $ratingsPartitions partitions)")
      println(s"  Load time: ${phase2Time}s")
      println()

      // PHASE 3: Cache the movies RDD and use it multiple times
      println("[PHASE 3] Caching movies RDD (to test cache detection)...")
      val phase3Start = System.currentTimeMillis()

      val cachedMovies = movies.cache()  // Add caching here
      val cacheCount1 = cachedMovies.count()  // Force cache population (1st access - no cache hit)

      val phase3Time = (System.currentTimeMillis() - phase3Start) / 1000.0
      println(s"  Movies cached: ${"%,d".format(cacheCount1)}")
      println(s"  Cache time: ${phase3Time}s")
      println()

      // PHASE 4: Reuse cached RDD - THIS SHOULD TRIGGER CACHE HIT
      println("[PHASE 4] Reusing cached movies RDD (should trigger cache hit detection)...")
      val phase4Start = System.currentTimeMillis()

      val cacheCount2 = cachedMovies.count()  // 2nd access - CACHE HIT!

      val phase4Time = (System.currentTimeMillis() - phase4Start) / 1000.0
      println(s"  Cache reuse count: ${"%,d".format(cacheCount2)}")
      println(s"  Cache reuse time: ${phase4Time}s")
      println()

      // PHASE 5: Filter cached movies (another cache hit)
      println("[PHASE 5] Filtering cached movies (another cache hit)...")
      val phase5Start = System.currentTimeMillis()

      val filteredMovies = cachedMovies
        .filter { case (id, _) => id % 10 == 0 }  // Filter - reads from cache
        .map(identity)  // Dummy map to create finals files

      val filteredCount = filteredMovies.count()

      val phase5Time = (System.currentTimeMillis() - phase5Start) / 1000.0
      println(s"  Filtered movies: ${"%,d".format(filteredCount)}")
      println(s"  Filter time: ${phase5Time}s")
      println()

      // PHASE 6: Join with ratings (uses cache for movies side)
      println("[PHASE 6] Joining cached movies with ratings...")
      val phase6Start = System.currentTimeMillis()

      val joinedData = cachedMovies.join(ratings)  // Movies side reads from cache
        .map(identity)

      val joinedCount = joinedData.count()

      val phase6Time = (System.currentTimeMillis() - phase6Start) / 1000.0
      println(s"  Join completed: ${"%,d".format(joinedCount)} movie-rating pairs")
      println(s"  Join time: ${phase6Time}s")
      println()

      // PHASE 7: Collect sample results
      println("[PHASE 7] Collecting sample results...")
      val phase7Start = System.currentTimeMillis()

      val sampleResults = joinedData.take(100)

      val phase7Time = (System.currentTimeMillis() - phase7Start) / 1000.0
      println(s"  Sample collected: ${sampleResults.length} records")
      println(s"  Collection time: ${phase7Time}s")
      println()

      // Display sample results
      println("=" * 80)
      println("SAMPLE JOINED RESULTS (First 10)")
      println("=" * 80)
      println(f"${"Movie ID"}%-10s ${"Title"}%-40s ${"User"}%-10s ${"Rating"}%8s")
      println("-" * 80)

      sampleResults.take(10).foreach { case (movieId, ((imdbId, language, title), (userId, rating, timestamp))) =>
        val truncatedTitle = if (title.length > 40) title.substring(0, 37) + "..." else title
        println(f"$movieId%-10d $truncatedTitle%-40s $userId%-10d $rating%8.1f")
      }
      println("=" * 80)
      println()

      val totalTime = (System.currentTimeMillis() - startTime) / 1000.0

      println("=" * 80)
      println("EXECUTION SUMMARY")
      println("=" * 80)
      println(f"Total movies:            ${"%,d".format(totalMovies)}")
      println(f"Total ratings:           ${"%,d".format(totalRatings)}")
      println(f"Filtered movies:         ${"%,d".format(filteredCount)}")
      println(f"Joined records:          ${"%,d".format(joinedCount)}")
      println(f"Phase 1 (Load Movies):   ${phase1Time}%.2fs")
      println(f"Phase 2 (Load Ratings):  ${phase2Time}%.2fs")
      println(f"Phase 3 (Cache):         ${phase3Time}%.2fs")
      println(f"Phase 4 (Cache Reuse):   ${phase4Time}%.2fs")
      println(f"Phase 5 (Filter):        ${phase5Time}%.2fs")
      println(f"Phase 6 (Join):          ${phase6Time}%.2fs")
      println(f"Phase 7 (Collect):       ${phase7Time}%.2fs")
      println(f"Total execution time:    ${totalTime}%.2fs")
      println("=" * 80)

    } finally {
      sc.stop()
    }
  }
}
