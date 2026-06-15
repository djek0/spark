package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object PerformanceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Performance Test")
    val sc = new SparkContext(conf)

    try {
      // Configuration: more partitions, larger data per partition
      val numPartitions = 20  // More tasks to create driver bottleneck
      val elementsPerPartition = 10000  // Larger data for Merkle advantage
      
      println(s"Starting test with $numPartitions partitions, $elementsPerPartition elements each")
      
      val startTime = System.currentTimeMillis()
      
      // Create RDD with many partitions and larger data
      val data = sc.parallelize(1 to (numPartitions * elementsPerPartition), numPartitions)
      
      // Complex transformation to add computational overhead
      val result = data.map { x =>
        // Simulate some computation
        val squared = x * x
        val cubed = squared * x
        (x, squared + cubed)
      }.reduceByKey(_ + _)
      
      // Collect results
      val collected = result.collect()
      
      val endTime = System.currentTimeMillis()
      val totalTime = (endTime - startTime) / 1000.0
      
      println(s"✓ Test completed successfully")
      println(s"  Total elements: ${collected.length}")
      println(s"  Total time: ${totalTime}s")
      
    } finally {
      sc.stop()
    }
  }
}
