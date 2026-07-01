package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Application 3: Movie Review Analysis
 *
 * Reads two files:
 * 1. Movies file (~1.7 MB): movieid,imdbid,language,title
 * 2. Ratings file: userid,movieid,rating,timestamp
 *
 * Performs an inner join on movieid and counts the number of reviews per movie title.
 *
 * Usage:
 *   ./bin/run-example MovieReviewAnalysis [partitions] [movies_file] [ratings_file] [output_dir]
 *
 * Parameters:
 *   partitions:    Number of partitions for Spark operations (default: auto-detect)
 *   movies_file:   Path to movies data file (required)
 *   ratings_file:  Path to ratings data file (required)
 *   output_dir:    Optional output directory for results
 *
 * Examples:
 *   ./bin/run-example MovieReviewAnalysis 16 /tmp/movies.txt /tmp/ratings.txt
 *   ./bin/run-example MovieReviewAnalysis 32 /tmp/movies.txt /tmp/ratings.txt /tmp/output
 *
 * To generate test data:
 *   ./bin/run-example MovieDataGenerator /tmp/movies.txt /tmp/ratings.txt
 */
object JoinCacheTester {

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

    val outputDir = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 3) Some(args(3)) else None
    } else {
      if (args.length > 2) Some(args(2)) else None
    }

    val conf = new SparkConf()
      .setAppName("Movie Review Analysis")
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
        println("Usage: ./bin/run-example MovieReviewAnalysis [partitions] [movies_file] [ratings_file] [output_dir]")
        println("")
        println("To generate test data:")
        println("  ./bin/run-example MovieDataGenerator /tmp/movies.txt /tmp/ratings.txt")
        println("")
        System.exit(1)
      }

      // Get Spark configuration info
      val defaultParallelism = sc.defaultParallelism
      val masterUrl = sc.master
      val numExecutors = sc.getExecutorMemoryStatus.size - 1  // Subtract driver

      println("=" * 80)
      println("MOVIE REVIEW ANALYSIS - Application 3")
      println("=" * 80)
      println(s"Spark Master:         $masterUrl")
      println(s"Default Parallelism:  $defaultParallelism partitions")
      println(s"Executors:            $numExecutors")
      println(s"Movies File:          ${moviesFile.get}")
      println(s"Ratings File:         ${ratingsFile.get}")
      if (outputDir.isDefined) {
        println(s"Output Directory:     ${outputDir.get}")
      }
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

      // PHASE 3: Perform Inner Join on movieid
      println("[PHASE 3] Performing inner join on movieid (triggers Byzantine verification)...")
      val phase3Start = System.currentTimeMillis()

      val joinedData = movies.join(ratings) // Inner join - SHUFFLE/VERIFICATION POINT!
        .map(identity)  // Dummy map to ensure outermost RDD is not a collapser (creates finals files)


      // Force execution and count joined records
      val joinedCount = joinedData.count()

      val phase3Time = (System.currentTimeMillis() - phase3Start) / 1000.0
      println(s"  Join completed: ${"%,d".format(joinedCount)} movie-rating pairs")
      println(s"  Join time: ${phase3Time}s")
      println()

      // PHASE 4: Count reviews per movie title
      println("[PHASE 4] Counting reviews per movie title (triggers Byzantine verification)...")
      val phase4Start = System.currentTimeMillis()

      val reviewCounts = joinedData
        .map { case (movieId, ((imdbId, language, title), (userId, rating, timestamp))) =>
          (title, 1)  // Extract title and count
        }
        .reduceByKey(_ + _)  // Aggregate - SHUFFLE/VERIFICATION POINT!

      val uniqueTitles = reviewCounts.count()

      val phase4Time = (System.currentTimeMillis() - phase4Start) / 1000.0
      println(s"  Aggregation completed: ${"%,d".format(uniqueTitles)} unique movie titles")
      println(s"  Aggregation time: ${phase4Time}s")
      println()

      // PHASE 5: Collect results and find top 100 movies
      println("[PHASE 5] Collecting and sorting movies by review count...")
      val phase5Start = System.currentTimeMillis()

      // Collect all results to driver (safe at the end of job)
      // Then sort in driver memory to avoid sortByKey's UID tracking issues
      val allResults = reviewCounts.collect()
      val top100 = allResults.sortBy(-_._2).take(100)

      val phase5Time = (System.currentTimeMillis() - phase5Start) / 1000.0
      println(s"  Collection and sorting completed")
      println(s"  Sort time: ${phase5Time}s")
      println()

      // Display results
      println("=" * 80)
      println("TOP 100 MOVIES BY NUMBER OF REVIEWS")
      println("=" * 80)
      println(f"${"Rank"}%-6s ${"Movie Title"}%-50s ${"Reviews"}%12s")
      println("-" * 80)

      top100.take(30).zipWithIndex.foreach { case ((title, count), idx) =>
        val rank = idx + 1
        val truncatedTitle = if (title.length > 50) title.substring(0, 47) + "..." else title
        println(f"$rank%-6d $truncatedTitle%-50s ${"%,d".format(count)}%12s")
      }
      if (top100.length > 30) {
        println(s"... (${top100.length - 30} more in top 100)")
      }
      println("=" * 80)
      println()

      // Statistics
      println("=" * 80)
      println("STATISTICS")
      println("=" * 80)
      val mostReviewed = top100.head
      val avgReviewsPerMovie = totalRatings.toDouble / uniqueTitles
      val moviesWithReviews = uniqueTitles
      val moviesWithoutReviews = totalMovies - uniqueTitles

      println(s"Most reviewed movie:     ${mostReviewed._1}")
      println(s"  Number of reviews:     ${"%,d".format(mostReviewed._2)}")
      println(s"Average reviews/movie:   ${"%,.1f".format(avgReviewsPerMovie)}")
      println(s"Movies with reviews:     ${"%,d".format(moviesWithReviews)}")
      println(s"Movies without reviews:  ${"%,d".format(moviesWithoutReviews)}")
      println(s"Total movies:            ${"%,d".format(totalMovies)}")
      println(s"Total ratings:           ${"%,d".format(totalRatings)}")
      println("=" * 80)
      println()

      // Save results if output directory specified
      outputDir.foreach { dir =>
        println(s"[OUTPUT] Saving results to: $dir")
        sc.parallelize(top100).saveAsTextFile(dir)
        println(s"  Results saved successfully")
        println()
      }

      val totalTime = (System.currentTimeMillis() - startTime) / 1000.0

      println("=" * 80)
      println("EXECUTION SUMMARY")
      println("=" * 80)
      println(f"Total movies:            ${"%,d".format(totalMovies)}")
      println(f"Total ratings:           ${"%,d".format(totalRatings)}")
      println(f"Joined records:          ${"%,d".format(joinedCount)}")
      println(f"Unique movie titles:     ${"%,d".format(uniqueTitles)}")
      println(f"Phase 1 (Load Movies):   ${phase1Time}%.2fs")
      println(f"Phase 2 (Load Ratings):  ${phase2Time}%.2fs")
      println(f"Phase 3 (Join):          ${phase3Time}%.2fs")
      println(f"Phase 4 (Count):         ${phase4Time}%.2fs")
      println(f"Phase 5 (Sort):          ${phase5Time}%.2fs")
      println(f"Total execution time:    ${totalTime}%.2fs")
      println("=" * 80)

    } finally {
      sc.stop()
    }
  }
}
