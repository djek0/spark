package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object PerformanceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Performance Test")
      .setMaster("local[*]")  // Use all available cores
      .set("spark.default.parallelism", "32")  // Increased for heavier workload
    val sc = new SparkContext(conf)

    try {
      // Configuration: heavy workload to show verification overhead
      val numPartitions = 32  // Increased for better parallelism
      val elementsPerPartition = 50000  // 1.6M total elements (32 * 50k)
      
      println(s"Starting test with $numPartitions partitions, $elementsPerPartition elements each")
      
      val startTime = System.currentTimeMillis()
      
      // Create RDD with heavy computation
      val data = sc.parallelize(1 to (numPartitions * elementsPerPartition), numPartitions)
      
      // Heavy transformation with shuffle (triggers verification)
      val result = data.map { x =>
        // Expensive computation (100 iterations)
        var sum = x.toDouble
        for (i <- 1 to 100) {
          sum = sum * 1.01 + math.sin(sum) + math.sqrt(math.abs(sum))
        }
        (x % 2000, sum)  // Create 2000 groups for shuffle
      }
      .groupByKey()  // Shuffle triggers Byzantine verification
      .mapValues { vals =>
        // Heavy aggregation
        val arr = vals.toArray
        var result = 0.0
        for (v <- arr; i <- 1 to 10) {
          result += v * i + math.log(math.abs(v) + 1)
        }
        result
      }
      
      // Collect results
      val collected = result.collect()
      
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime) / 1000.0
      
      println(s"[SUCCESS] Test completed successfully")
      println(s"  Total elements: ${collected.length}")
      println(s"  Total time: ${totalTime}s")
      
    } finally {
      sc.stop()
    }
  }
}
