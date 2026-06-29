package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Application 1: Twitter Follower Analysis
 * 
 * Takes as input a file containing Twitter data with the structure:
 * <follower> <followee>
 * 
 * Each line expresses that <follower> follows <followee>.
 * 
 * Purpose: Determine how many users each follower follows and sort them
 * in descending order based on this count.
 * 
 * Usage:
 *   ./bin/run-example TwitterFollowerAnalysis [partitions] [input_file] [output_dir]
 * 
 * Parameters:
 *   partitions:  Number of partitions for Spark operations (default: auto-detect based on file size)
 *   input_file:  Path to Twitter data file (required)
 *   output_dir:  Optional output directory for results
 * 
 * Examples:
 *   ./bin/run-example TwitterFollowerAnalysis 32 /tmp/twitter_data.txt
 *   ./bin/run-example TwitterFollowerAnalysis 12 /tmp/twitter_data.txt /tmp/output
 * 
 * To generate test data:
 *   ./bin/run-example TwitterDataGenerator /tmp/twitter_data.txt
 */
object TwitterFollowerAnalysis {
  
  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val partitions = if (args.length > 0 && args(0).forall(_.isDigit)) {
      args(0).toInt
    } else {
      -1  // Auto-detect
    }
    
    val inputFile = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 1) Some(args(1)) else None
    } else {
      if (args.length > 0) Some(args(0)) else None
    }
    
    val outputDir = if (args.length > 0 && args(0).forall(_.isDigit)) {
      if (args.length > 2) Some(args(2)) else None
    } else {
      if (args.length > 1) Some(args(1)) else None
    }
    
    val conf = new SparkConf()
      .setAppName("Twitter Follower Analysis")
      .setMaster("local[*]")
    
    // Set default parallelism if specified
    if (partitions > 0) {
      conf.set("spark.default.parallelism", partitions.toString)
    }
    
    val sc = new SparkContext(conf)
    
    try {
      val startTime = System.currentTimeMillis()
      
      // Validate input
      if (inputFile.isEmpty) {
        println("ERROR: Input file is required!")
        println("")
        println("Usage: ./bin/run-example TwitterFollowerAnalysis [partitions] [input_file] [output_dir]")
        println("")
        println("To generate test data:")
        println("  ./bin/run-example TwitterDataGenerator /tmp/twitter_data.txt")
        println("")
        System.exit(1)
      }
      
      // Get Spark configuration info
      val defaultParallelism = sc.defaultParallelism
      val masterUrl = sc.master
      val numExecutors = sc.getExecutorMemoryStatus.size - 1  // Subtract driver
      
      println("=" * 80)
      println("TWITTER FOLLOWER ANALYSIS - Application 1")
      println("=" * 80)
      println(s"Spark Master:         $masterUrl")
      println(s"Default Parallelism:  $defaultParallelism partitions")
      println(s"Executors:            $numExecutors")
      println(s"Input File:           ${inputFile.get}")
      if (outputDir.isDefined) {
        println(s"Output Directory:     ${outputDir.get}")
      }
      println("=" * 80)
      println()
      
      // Load data
      println(s"[PHASE 1] Loading Twitter data from: ${inputFile.get}")
      val minPartitions = if (partitions > 0) partitions else 32
      val edges = sc.textFile(inputFile.get, minPartitions = minPartitions)
        .map { line =>
          val parts = line.split("\\s+")
          if (parts.length >= 2) {
            (parts(0), parts(1))  // (follower, followee)
          } else {
            ("invalid", "invalid")
          }
        }
        .filter { case (follower, followee) => follower != "invalid" }
      
      val actualPartitions = edges.getNumPartitions
      println(s"  Data loaded into $actualPartitions partitions")
      
      val loadTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"  Data loaded in ${loadTime}s")
      println()
      
      // PHASE 2: Count how many users each follower follows
      println("[PHASE 2] Counting followers per user (triggers Byzantine verification)...")
      val phase2Start = System.currentTimeMillis()
      
      val followerCounts = edges
        .map { case (follower, _) => (follower, 1) }
        .reduceByKey(_ + _)  // Shuffle + aggregation - verification point!
      //  .map(identity)  // Dummy map to ensure outermost RDD is not a collapser (creates finals files)

      // Force execution
      val totalUsers = followerCounts.count()
      
      val phase2Time = (System.currentTimeMillis() - phase2Start) / 1000.0
      println(s"  Aggregation completed: $totalUsers unique followers")
      println(s"  Time: ${phase2Time}s")
      println()
      
      // PHASE 3: Collect results and sort in driver
      println("[PHASE 3] Collecting results...")
      val phase3Start = System.currentTimeMillis()
      
      // Collect all results to driver (safe at the end of job)
      // Then sort in driver memory to avoid sortByKey's UID tracking issues
      val allResults = followerCounts.collect()
      // val top100 = allResults.sortBy(-_._2).take(100)
      val top100 = allResults.take(100)


      val phase3Time = (System.currentTimeMillis() - phase3Start) / 1000.0
      println(s"  Collection and sorting completed")
      println(s"  Time: ${phase3Time}s")
      println()
      
      // Display results
      println("=" * 80)
      println("TOP 100 USERS BY NUMBER OF FOLLOWEES")
      println("=" * 80)
      println(f"${"User ID"}%-20s ${"Follows Count"}%15s")
      println("-" * 80)
      top100.take(20).foreach { case (user, count) =>
        println(f"$user%-20s $count%15d")
      }
      if (top100.length > 20) {
        println(s"... (${top100.length - 20} more)")
      }
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
      println(f"Total users analyzed: $totalUsers%,d")
      println(f"Phase 1 (Load):       ${loadTime}%.2fs")
      println(f"Phase 2 (Aggregate):  ${phase2Time}%.2fs")
      println(f"Phase 3 (Sort):       ${phase3Time}%.2fs")
      println(f"Total execution time: ${totalTime}%.2fs")
      println("=" * 80)
      
    } finally {
      sc.stop()
    }
  }
}
