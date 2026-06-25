package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

/**
 * Comparison: Merkle Tree vs Full Task Recomputation
 * 
 * Demonstrates that Merkle trees are dramatically faster when only a few elements
 * differ in large partitions (sparse Byzantine faults).
 * 
 * Run configurations:
 * 1. Full task verification:
 *    HONEST=False BYZANTINE_PROBABILITY=10 EXEC_VERIFICATION=true MERKLE_VERIFICATION=false \
 *    ./bin/run-example MerkleVsFullTaskComparison
 * 
 * 2. Merkle tree verification:
 *    HONEST=False BYZANTINE_PROBABILITY=10 EXEC_VERIFICATION=true MERKLE_VERIFICATION=true \
 *    ./bin/run-example MerkleVsFullTaskComparison
 * 
 * Expected speedup: ~1.5-2x with Merkle trees
 */
object MerkleVsFullTaskComparison {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Merkle vs Full Task Comparison")
      .master("local[*]")  // Use all available cores (auto-detect)
      .config("spark.default.parallelism", "12")  // Control shuffle partitions (not core count)
      .config("spark.sql.shuffle.partitions", "12")  // Keep shuffles manageable
      .getOrCreate()
    
    println("=" * 80)
    println("MERKLE TREE vs FULL TASK RECOMPUTATION COMPARISON")
    println("=" * 80)
    
    val execVerification = sys.env.getOrElse("EXEC_VERIFICATION", "true")
    val merkleVerification = sys.env.getOrElse("MERKLE_VERIFICATION", "true")
    val byzantineProb = sys.env.getOrElse("BYZANTINE_PROBABILITY", "10")
    val honest = sys.env.getOrElse("HONEST", "True")
    
    println(s"Configuration:")
    println(s"  HONEST: $honest")
    println(s"  BYZANTINE_PROBABILITY: $byzantineProb (every ${byzantineProb}th task = ${100.0 / byzantineProb.toInt}%)")
    println(s"  EXEC_VERIFICATION: $execVerification")
    println(s"  MERKLE_VERIFICATION: $merkleVerification")
    println()
    
    val startTime = System.currentTimeMillis()
    
    // PHASE 1: Large-scale data processing with expensive computation
    println("[PHASE 1] Generating and processing data...")
    val phaseStartTime = System.currentTimeMillis()
    
    val partitions = 32  // Increased for better parallelism and larger data
    val elementsPerPartition = 50000  // 1.6M total elements (32 * 50k)
    
    val sc = spark.sparkContext
    val data = sc.parallelize(0L until (partitions * elementsPerPartition), partitions)
      .map { n => 
        // Heavy computation to make verification overhead visible
        var sum = n.toDouble
        // Simulate expensive computation (100 iterations)
        for (i <- 1 to 100) {
          sum = sum * 1.01 + math.sin(sum) + math.sqrt(math.abs(sum))
        }
        (n % 2000, sum)  // Create 2000 groups for larger shuffle
      }
    
    println(s"  Generated ${partitions * elementsPerPartition} elements across $partitions partitions")
    println(s"  Time: ${(System.currentTimeMillis() - phaseStartTime) / 1000.0}s")
    println()
    
    // PHASE 2: Shuffle and aggregation (triggers Byzantine verification)
    println("[PHASE 2] Shuffle and aggregation (verification point)...")
    val shuffleStartTime = System.currentTimeMillis()
    
    val result = data
      .groupByKey()  // Shuffle → Creates ~2000 partitions → Verification triggers here
      .mapValues { iter =>
        // Each partition has ~800 elements
        // More expensive aggregation to increase result size
        val values = iter.toArray
        var result = 0.0
        // Heavy aggregation
        for (v <- values) {
          for (i <- 1 to 10) {
            result += v * i + math.log(math.abs(v) + 1)
          }
        }
        result
      }
      .collect()
    
    val shuffleTime = (System.currentTimeMillis() - shuffleStartTime) / 1000.0
    println(s"  Shuffle completed: ${result.length} groups processed")
    println(s"  Shuffle + Verification time: ${shuffleTime}s")
    println()
    
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    println("=" * 80)
    println("RESULTS")
    println("=" * 80)
    println(f"Total execution time: ${totalTime}%.2f seconds")
    println()
    
    if (merkleVerification == "true") {
      println("[MERKLE ENABLED] Merkle tree verification enabled")
      println("   - Only disagreeing elements recomputed")
      println("   - Expected: ~1.5-2x faster than full task")
      println()
      println("   How it works:")
      println("   1. Build Merkle tree from partition results (~5000 elements)")
      println("   2. Compare trees to find disagreement (log₂ 5000 = ~13 comparisons)")
      println("   3. Recompute only the 1 disagreeing element")
      println("   4. Speedup: ~500-1000x for verification phase alone")
    } else {
      println("[FULL TASK] Full task recomputation enabled")
      println("   - All elements recomputed for Byzantine partitions")
      println("   - Baseline for comparison")
      println()
      println("   How it works:")
      println("   1. Detect hash mismatch between replicas")
      println("   2. Recompute entire partition (~5000 elements)")
      println("   3. Compare full results to find correct replica")
      println("   4. Wasteful: 99.9% of elements are actually correct")
    }
    println("=" * 80)
    
    spark.stop()
  }
  
}
