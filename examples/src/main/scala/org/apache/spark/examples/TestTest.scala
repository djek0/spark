package org.apache.spark.examples

import java.io.File
import org.apache.spark.sql.SparkSession

object TestTest {
  def main(args: Array[String]): Unit = {
    println("=== Starting UID Tracking Test ===")
    
    val spark = SparkSession.builder()
      .appName("UID_Tracking_Test")
      .master("local[2]")  // Use local mode for testing
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()
      
      println("\n=== Test 1: Basic Map and Filter Operations ===")
      testBasicOperations(sc)
      
      println("\n=== Test 2: Multiple Filter Operations ===")
      testMultipleFilters(sc)
      
      println("\n=== Test 3: Different Actions (collect, count, reduce) ===")
      testDifferentActions(sc)
      
      println("\n=== Test 4: Complex Pipeline with Shuffle ===")
      testComplexPipeline(sc)
      
      // Check log files
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()
      
    } finally {
      spark.stop()
    }
    
    println("\n=== UID Tracking Test Complete ===")
  }
  
  def testBasicOperations(sc: org.apache.spark.SparkContext): Unit = {
    println("Creating RDD with 1-10, mapping x2, filtering even numbers")
    
    val input = sc.parallelize(1 to 10, 2)
    val mapped = input.map(_ * 2)
    val filtered = mapped.filter(_ % 4 == 0)  // Only numbers divisible by 4
    val result = filtered.collect()
    
    println(s"Input: ${(1 to 10).toList}")
    println(s"After map(*2): ${(1 to 10).map(_ * 2).toList}")
    println(s"After filter(%4==0): ${result.toList}")
    println(s"Expected: [4, 8, 12, 16, 20], Got: ${result.toList}")
  }
  
  def testMultipleFilters(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing multiple consecutive filters")
    
    val input = sc.parallelize(1 to 20, 2)
    val result = input
      .filter(_ > 5)        // Remove 1,2,3,4,5
      .filter(_ < 15)       // Remove 15,16,17,18,19,20
      .filter(_ % 2 == 0)   // Keep only even numbers
      .collect()
    
    println(s"Input: ${(1 to 20).toList}")
    println(s"After filters (>5, <15, even): ${result.toList}")
    println(s"Expected: [6, 8, 10, 12, 14], Got: ${result.toList}")
  }
  
  def testDifferentActions(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing different actions: collect, count, reduce")
    
    val input = sc.parallelize(1 to 5, 2)
    val processed = input.map(_ * 2)
    
    // Test collect
    val collected = processed.collect()
    println(s"Collect result: ${collected.toList} Expected: [2, 4, 6, 8, 10]")
    
    // Test count
    val count = processed.count()
    println(s"Count result: $count Expected: 5")
    
    // Test reduce
    val sum = processed.reduce(_ + _)
    println(s"Reduce (sum) result: $sum Expected: 30")
  }
  
  def testComplexPipeline(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing complex pipeline with shuffle")
    
    // Stage 1: Parallelize input (root RDD)
    val input = sc.parallelize(1 to 10, 2)

    // Map (narrow transformation)
    val mapped = input.map(x => (x % 3, x * 2))

    // Filter (still narrow)
    val filtered = mapped.filter { case (_, v) => v > 5 }

    // Stage 2: groupByKey (wide dependency, triggers a shuffle)
    val grouped = filtered.groupByKey()

    // Add a map and a filter after groupByKey
    val processed = grouped
      .map { case (key, values) =>
        val sum = values.sum
        (key, sum)
      }
      .filter { case (_, sum) => sum > 10 }

    // Final action: collect and print
    val result = processed.collect()

    println("== Complex Pipeline Results ==")
    result.foreach { case (key, sum) =>
      println(s"Group $key: sum = $sum")
    }
    println("== Expected Results: 36, 42, 26 ==")
  }
  
  def cleanupLogFiles(): Unit = {
    val currentDir = new File(".")
    val logFiles = currentDir.listFiles().filter { file =>
      file.getName.startsWith("spark_inputs_") || file.getName.startsWith("spark_finals_")
    }
    logFiles.foreach(_.delete())
    println(s"Cleaned up ${logFiles.length} existing log files")
  }
  
  def checkLogFiles(): Unit = {
    val currentDir = new File(".")
    val inputLogs = currentDir.listFiles().filter(_.getName.startsWith("spark_inputs_"))
    val outputLogs = currentDir.listFiles().filter(_.getName.startsWith("spark_finals_"))
    
    println(s"Found ${inputLogs.length} input log files:")
    inputLogs.foreach(file => println(s"  - ${file.getName}"))
    
    println(s"Found ${outputLogs.length} output log files:")
    outputLogs.foreach(file => println(s"  - ${file.getName}"))
    
    // Show content of first few log files
    (inputLogs ++ outputLogs).take(3).foreach { file =>
      println(s"\nContent of ${file.getName}:")
      try {
        val lines = scala.io.Source.fromFile(file).getLines().take(5).toList
        lines.foreach(line => println(s"  $line"))
        if (scala.io.Source.fromFile(file).getLines().length > 5) {
          println("  ...")
        }
      } catch {
        case e: Exception => println(s"  Error reading file: ${e.getMessage}")
      }
    }
  }
}
