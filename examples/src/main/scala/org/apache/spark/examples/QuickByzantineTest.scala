package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Quick Byzantine test with controlled partition count.
 * Shows the difference between too many vs reasonable partitions.
 */
object QuickByzantineTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Quick Byzantine Test")
      .setMaster("local[*]")  // Use all available cores
      .set("spark.default.parallelism", "4")  // Control shuffle partitions (keeps test fast)
    val sc = new SparkContext(conf)

    try {
      val startTime = System.currentTimeMillis()
      
      // Small dataset: 4 partitions with 100 elements each
      val numPartitions = 4
      val elementsPerPartition = 100
      
      println(s"[CONFIG] Partitions: $numPartitions, Elements: $elementsPerPartition")
      println(s"[CONFIG] spark.default.parallelism: ${conf.get("spark.default.parallelism", "not set")}")
      println(s"[CONFIG] HONEST: ${sys.env.getOrElse("HONEST", "True")}")
      println(s"[CONFIG] BYZANTINE_PROBABILITY: ${sys.env.getOrElse("BYZANTINE_PROBABILITY", "N/A")}")
      println(s"[CONFIG] EXEC_VERIFICATION: ${sys.env.getOrElse("EXEC_VERIFICATION", "true")}")
      println(s"[CONFIG] MERKLE_VERIFICATION: ${sys.env.getOrElse("MERKLE_VERIFICATION", "true")}")
      
      // Create RDD
      val data = sc.parallelize(1 to (numPartitions * elementsPerPartition), numPartitions)
      
      // Transform to key-value pairs
      val pairs = data.map(x => (x % 10, x * 2))
      
      println(s"\n[STAGE 1] Created ${pairs.getNumPartitions} partitions")
      
      // Shuffle operation - this will create spark.default.parallelism partitions
      val result = pairs.reduceByKey(_ + _)
      
      println(s"[STAGE 2] After shuffle: ${result.getNumPartitions} partitions")
      
      // Collect results
      val collected = result.collect()
      
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime) / 1000.0
      
      println(s"\n[SUCCESS] Test completed")
      println(s"  Results: ${collected.length} groups")
      println(s"  Total time: ${totalTime}s")
      
      collected.sortBy(_._1).foreach { case (k, v) =>
        println(s"  Group $k: $v")
      }
      
    } finally {
      sc.stop()
    }
  }
}
