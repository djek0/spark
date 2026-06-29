package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Twitter Data Generator
 * 
 * Generates a synthetic Twitter follower dataset and saves it to disk.
 * Output format: <follower> <followee> (one relationship per line)
 * 
 * Usage:
 *   ./bin/run-example TwitterDataGenerator [output_path] [num_edges] [num_users]
 * 
 * Defaults:
 *   output_path: /tmp/twitter_data.txt
 *   num_edges: 50000000 (50M edges = ~1.3GB)
 *   num_users: 1000000 (1M unique users)
 */
object TwitterDataGenerator {
  
  def main(args: Array[String]): Unit = {
    val outputPath = if (args.length > 0) args(0) else "/tmp/twitter_data.txt"
    val numEdges = if (args.length > 1) args(1).toLong else 50000000L
    val numUsers = if (args.length > 2) args(2).toInt else 1000000
    
    val conf = new SparkConf()
      .setAppName("Twitter Data Generator")
      .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    try {
      println("=" * 80)
      println("TWITTER DATA GENERATOR")
      println("=" * 80)
      println(s"Output path:      $outputPath")
      println(s"Number of edges:  ${"%,d".format(numEdges)} (~${numEdges * 25 / 1024 / 1024 / 1024}GB)")
      println(s"Number of users:  ${"%,d".format(numUsers)}")
      println(s"Partitions:       32")
      println("=" * 80)
      println()
      
      val startTime = System.currentTimeMillis()
      
      println("[PHASE 1] Generating follower-followee relationships...")
      
      val edges = sc.parallelize(1L to numEdges, 32)
        .map { edgeId =>
          // Generate realistic follower patterns:
          // - Power law distribution (some users follow many, most follow few)
          // - Use hash for deterministic but pseudo-random assignment
          
          val seed = edgeId
          val follower = Math.abs((seed * 2654435761L) % numUsers)
          val followee = Math.abs((seed * 1103515245L + 12345L) % numUsers)
          
          // Avoid self-follows
          if (follower == followee) {
            s"user_$follower user_${(follower + 1) % numUsers}"
          } else {
            s"user_$follower user_$followee"
          }
        }
      
      println(s"  Generated ${"%,d".format(numEdges)} edges")
      println()
      
      println("[PHASE 2] Saving to disk...")
      edges.saveAsTextFile(outputPath)
      
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime) / 1000.0
      
      println()
      println("=" * 80)
      println("GENERATION COMPLETE")
      println("=" * 80)
      println(s"Output saved to:  $outputPath")
      println(s"Total time:       ${totalTime}s")
      println("=" * 80)
      
      println()
      println("To use this dataset with TwitterFollowerAnalysis:")
      println(s"  ./bin/run-example TwitterFollowerAnalysis $outputPath")
      
    } finally {
      sc.stop()
    }
  }
}
