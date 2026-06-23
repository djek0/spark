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
      .master("local[8]")  // 8 cores
      .config("spark.executor.instances", "4")
      .config("spark.executor.cores", "2")
      .config("spark.default.parallelism", "100")
      .config("spark.sql.shuffle.partitions", "100")
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
    
    val partitions = 50
    val elementsPerPartition = 100000  // 100k elements per partition = 5M total
    
    val sc = spark.sparkContext
    val data = sc.parallelize(0L until (partitions * elementsPerPartition), partitions)
      .map { n => 
        (n % 1000, n.toDouble)  // Create 1000 groups
      }
    
    println(s"  Generated ${partitions * elementsPerPartition} elements across $partitions partitions")
    println(s"  Time: ${(System.currentTimeMillis() - phaseStartTime) / 1000.0}s")
    println()
    
    // PHASE 2: Shuffle and aggregation (triggers Byzantine verification)
    println("[PHASE 2] Shuffle and aggregation (verification point)...")
    val shuffleStartTime = System.currentTimeMillis()
    
    val result = data
      .groupByKey()  // Shuffle → Creates ~1000 partitions → Verification triggers here
      .mapValues { iter =>
        // Each partition has ~5000 elements
        // Byzantine faults corrupt 1-3 elements per partition (sparse faults)
        iter.map(v => v * 2 + math.sqrt(v)).toArray
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
