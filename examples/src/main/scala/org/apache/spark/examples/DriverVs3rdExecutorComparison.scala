package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

/**
 * Comparison: Driver vs 3rd Executor Verification
 * 
 * Demonstrates that driver becomes a severe bottleneck under high verification load.
 * Multi-stage pipeline with many partitions creates verification pressure.
 * 
 * Run configurations:
 * 1. Driver verification (shows bottleneck):
 *    HONEST=False BYZANTINE_PROBABILITY=5 EXEC_VERIFICATION=false MERKLE_VERIFICATION=true \
 *    ./bin/run-example DriverVs3rdExecutorComparison
 * 
 * 2. 3rd executor verification (distributed load):
 *    HONEST=False BYZANTINE_PROBABILITY=5 EXEC_VERIFICATION=true MERKLE_VERIFICATION=true \
 *    ./bin/run-example DriverVs3rdExecutorComparison
 * 
 * Expected speedup: ~2-3x with 3rd executor verification
 * 
 * Optional: Add CONCURRENT_JOBS=4 to run multiple jobs in parallel (extreme stress test)
 */
object DriverVs3rdExecutorComparison {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Driver vs 3rd Executor Comparison")
      .master("local[8]")  // 8 cores
      .config("spark.executor.instances", "4")
      .config("spark.executor.cores", "2")
      .config("spark.default.parallelism", "200")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()
    
    println("=" * 80)
    println("DRIVER vs 3RD EXECUTOR VERIFICATION COMPARISON")
    println("=" * 80)
    
    val execVerification = sys.env.getOrElse("EXEC_VERIFICATION", "true")
    val merkleVerification = sys.env.getOrElse("MERKLE_VERIFICATION", "true")
    val byzantineProb = sys.env.getOrElse("BYZANTINE_PROBABILITY", "5")
    val honest = sys.env.getOrElse("HONEST", "True")
    val concurrentJobs = sys.env.getOrElse("CONCURRENT_JOBS", "1").toInt
    
    println(s"Configuration:")
    println(s"  HONEST: $honest")
    println(s"  BYZANTINE_PROBABILITY: $byzantineProb (${100.0 / byzantineProb.toInt}% Byzantine)")
    println(s"  EXEC_VERIFICATION: $execVerification ${if (execVerification == "false") "(DRIVER)" else "(3RD EXECUTOR)"}")
    println(s"  MERKLE_VERIFICATION: $merkleVerification")
    println(s"  CONCURRENT_JOBS: $concurrentJobs")
    println()
    
    if (execVerification == "false") {
      println("[WARNING] Driver verification enabled")
      println("   Expect driver CPU saturation and scheduling delays")
      println()
    }
    
    val startTime = System.currentTimeMillis()
    
    // Run jobs (sequential or parallel based on config)
    if (concurrentJobs == 1) {
      runPipeline(spark, jobId = 1)
    } else {
      println(s"Running $concurrentJobs concurrent jobs to maximize verification pressure...")
      println()
      
      (1 to concurrentJobs).par.foreach { jobId =>
        runPipeline(spark, jobId)
      }
    }
    
    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    
    println()
    println("=" * 80)
    println("RESULTS")
    println("=" * 80)
    println(f"Total execution time: ${totalTime}%.2f seconds")
    println(f"Time per job (avg): ${totalTime / concurrentJobs}%.2f seconds")
    println()
    
    if (execVerification == "false") {
      println("[DRIVER VERIFICATION]:")
      println("   - All verification traffic flows through driver")
      println("   - Driver handles task scheduling + verification recomputation")
      println("   - Bottleneck: serialization, CPU, network")
      println()
      println("   Expected observations:")
      println("   - Driver CPU: 100% (saturated)")
      println("   - Task scheduling delays: +20-50ms")
      println("   - Slower overall execution")
      println()
      println("   Why it's slow:")
      println("   1. Network: All verification data centralized at driver")
      println("   2. CPU: Driver doing scheduling + 160 verifications")
      println("   3. Serialization: 160 task results serialized/deserialized")
      println("   4. Single point of failure: driver can't scale")
    } else {
      println("[3RD EXECUTOR VERIFICATION]:")
      println("   - Verification distributed across executor nodes")
      println("   - Driver only handles task scheduling")
      println("   - No bottleneck")
      println()
      println("   Expected observations:")
      println("   - Driver CPU: 30-40% (healthy)")
      println("   - No scheduling delays")
      println("   - ~2-3x faster than driver verification")
      println()
      println("   Why it's faster:")
      println("   1. Network: Verification stays between executors")
      println("   2. CPU: Load distributed across multiple nodes")
      println("   3. Parallelism: Verifications run concurrently")
      println("   4. Scalability: Scales with executor count")
    }
    println("=" * 80)
    
    spark.stop()
  }
  
  /**
   * Multi-stage shuffle pipeline.
   * Each shuffle boundary triggers verification for Byzantine partitions.
   * 
   * With BYZANTINE_PROBABILITY=5 (20% Byzantine) and 200 partitions:
   * - ~40 verifications per stage
   * - 4 stages = 160 total verifications
   * - This creates significant load on driver if EXEC_VERIFICATION=false
   */
  def runPipeline(spark: SparkSession, jobId: Int): Unit = {
    println(s"[JOB $jobId] Starting multi-stage pipeline...")
    val jobStartTime = System.currentTimeMillis()
    
    val partitions = 200
    val totalElements = 10000000  // 10M elements
    
    val sc = spark.sparkContext
    
    // STAGE 1: Initial grouping
    println(s"[JOB $jobId] Stage 1: Initial grouping ($totalElements elements, $partitions partitions)")
    val stage1Start = System.currentTimeMillis()
    
    val stage1 = sc.parallelize(0L until totalElements, partitions)
      .map(n => (n % 10000, n))
      .groupByKey()  // Shuffle → Verification point 1
    
    println(s"[JOB $jobId] Stage 1 completed in ${(System.currentTimeMillis() - stage1Start) / 1000.0}s")
    
    // STAGE 2: Reduce aggregation
    println(s"[JOB $jobId] Stage 2: Reduce aggregation")
    val stage2Start = System.currentTimeMillis()
    
    val stage2 = stage1
      .mapValues(_.foldLeft(0L)(_ + _))
      .reduceByKey(_ + _)  // Shuffle → Verification point 2
    
    println(s"[JOB $jobId] Stage 2 completed in ${(System.currentTimeMillis() - stage2Start) / 1000.0}s")
    
    // STAGE 3: Re-partitioning
    println(s"[JOB $jobId] Stage 3: Re-partitioning")
    val stage3Start = System.currentTimeMillis()
    
    val stage3 = stage2
      .map { case (k, v) => (k % 100, v) }
      .groupByKey()  // Shuffle → Verification point 3
    
    println(s"[JOB $jobId] Stage 3 completed in ${(System.currentTimeMillis() - stage3Start) / 1000.0}s")
    
    // STAGE 4: Final aggregation and collection
    println(s"[JOB $jobId] Stage 4: Final aggregation")
    val stage4Start = System.currentTimeMillis()
    
    val result = stage3
      .mapValues(_.foldLeft(0L)(_ + _))
      .collect()  // Triggers final computation
    
    println(s"[JOB $jobId] Stage 4 completed in ${(System.currentTimeMillis() - stage4Start) / 1000.0}s")
    
    val jobTime = (System.currentTimeMillis() - jobStartTime) / 1000.0
    println(s"[JOB $jobId] Pipeline completed: ${result.length} groups, ${jobTime}s total")
  }
}
