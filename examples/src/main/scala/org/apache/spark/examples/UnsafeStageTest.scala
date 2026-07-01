package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Unsafe Stage Test - Verifies UNSAFE_STAGE recomputation path
 *
 * This test verifies that when both replicas hit a collapser (join),
 * they write UNSAFE_STAGE markers and trigger full task recomputation
 * instead of Merkle tree verification.
 *
 * Usage:
 *   HONEST=False BYZANTINE_PROBABILITY=10 EXEC_VERIFICATION=true MERKLE_VERIFICATION=true \
 *     ./bin/run-example UnsafeStageTest [partitions] [movies_file] [ratings_file]
 *
 * Parameters:
 *   partitions:    Number of partitions (default: 16)
 *   movies_file:   Path to movies data file (default: /tmp/movies.txt)
 *   ratings_file:  Path to ratings data file (default: /tmp/ratings.txt)
 */
object UnsafeStageTest {

  def main(args: Array[String]): Unit = {
    val partitions = if (args.length > 0 && args(0).forall(_.isDigit)) {
      args(0).toInt
    } else {
      16
    }

    val moviesFile = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 1) args(1) else "/tmp/movies.txt"
    } else {
      if (args.length > 0) args(0) else "/tmp/movies.txt"
    }

    val ratingsFile = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 2) args(2) else "/tmp/ratings.txt"
    } else {
      if (args.length > 1) args(1) else "/tmp/ratings.txt"
    }

    val conf = new SparkConf()
      .setAppName("Unsafe Stage Test")
      .setMaster("local[*]")

    if (partitions > 0) {
      conf.set("spark.default.parallelism", partitions.toString)
    }

    val sc = new SparkContext(conf)

    try {
      println(s"================================================================================")
      println(s"UNSAFE STAGE RECOMPUTATION TEST")
      println(s"================================================================================")
      println(s"Partitions: $partitions")
      println(s"Movies file: $moviesFile")
      println(s"Ratings file: $ratingsFile")
      println(s"================================================================================")
      println()

      val phase1Start = System.currentTimeMillis()

      // Load movies (stage root)
      val movies = sc.textFile(moviesFile, partitions)
        .map { line =>
          val parts = line.split(",")
          (parts(0).toInt, parts(1))
        }

      val phase1Time = (System.currentTimeMillis() - phase1Start) / 1000.0
      println(s"[PHASE 1] Loaded movies RDD")
      println(s"  Time: ${phase1Time}s")
      println()

      val phase2Start = System.currentTimeMillis()

      // Load ratings (stage root)
      val ratings = sc.textFile(ratingsFile, partitions)
        .map { line =>
          val parts = line.split(",")
          (parts(1).toInt, (parts(0).toInt, parts(2).toDouble))
        }

      val phase2Time = (System.currentTimeMillis() - phase2Start) / 1000.0
      println(s"[PHASE 2] Loaded ratings RDD")
      println(s"  Time: ${phase2Time}s")
      println()

      val phase3Start = System.currentTimeMillis()

      // Join operations - COLLAPSER!
      // This should trigger UNSAFE_STAGE markers in both replicas
      val joined = movies.join(ratings, partitions)
        .map { case (movieId, (title, (userId, rating))) =>
          (movieId, title, userId, rating)
        }

      val phase3Time = (System.currentTimeMillis() - phase3Start) / 1000.0
      println(s"[PHASE 3] Performed join (COLLAPSER - should trigger UNSAFE_STAGE)")
      println(s"  Time: ${phase3Time}s")
      println()

      val phase4Start = System.currentTimeMillis()

      // Count to trigger execution
      val count = joined.count()

      val phase4Time = (System.currentTimeMillis() - phase4Start) / 1000.0
      println(s"[PHASE 4] Counted joined records")
      println(s"  Count: ${"%,d".format(count)}")
      println(s"  Time: ${phase4Time}s")
      println()

      println(s"================================================================================")
      println(s"EXECUTION SUMMARY")
      println(s"================================================================================")
      println(s"Total joined records: ${"%,d".format(count)}")
      println(s"Phase 1 (Load Movies):   ${phase1Time}s")
      println(s"Phase 2 (Load Ratings):  ${phase2Time}s")
      println(s"Phase 3 (Join - COLLAPSER): ${phase3Time}s")
      println(s"Phase 4 (Count):         ${phase4Time}s")
      println(s"Total execution time:    ${phase1Time + phase2Time + phase3Time + phase4Time}s")
      println(s"================================================================================")
      println()
      println(s"EXPECTED BEHAVIOR:")
      println(s"  - Both replicas should write UNSAFE_STAGE markers")
      println(s"  - Verification manager should detect UNSAFE_STAGE in both replicas")
      println(s"  - Full task recomputation should be triggered")
      println(s"  - Look for 'MerkleBuildFailure' or 'recomputation' in logs")
      println(s"================================================================================")

    } finally {
      sc.stop()
    }
  }
}
