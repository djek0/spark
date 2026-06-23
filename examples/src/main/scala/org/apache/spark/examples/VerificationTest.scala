package org.apache.spark.examples

import org.apache.spark.{SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.sql.SparkSession
import org.apache.spark.executor.TaskMetrics

object VerificationTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("VerificationTest")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext
    
    println("\n=== Test 1: Normal Mode (No Verification) ===")
    val numbers = sc.parallelize(1 to 10, 2)
    val doubled = numbers.map(_ * 2)
    val result1 = doubled.collect()
    println(s"Normal mode result: ${result1.mkString(", ")}")
    
    println("\n=== Test 2: Verification Mode (Single Element) ===")
    // Simulate verification by manually creating a TaskContext with targetElementId
    testSingleElementVerification(sc)
    
    spark.stop()
  }
  
  def testSingleElementVerification(sc: org.apache.spark.SparkContext): Unit = {
    println("Creating RDD for verification test...")
    val numbers = sc.parallelize(1 to 10, 1)  // Single partition for simplicity
    val rdd = numbers.map(_ * 2)
    
    // Get partition
    val partition = rdd.partitions(0)
    
    // Create verification context with targetElementId = 3
    val verificationContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = -999L,
      attemptNumber = 0,
      taskIndex = 0,
      taskMemoryManager = null,
      localProperties = new java.util.Properties(),
      metricsSystem = SparkEnv.get.metricsSystem,
      taskMetrics = TaskMetrics.empty,
      resources = Map.empty,
      targetElementId = Some(3L)  // Only process element with UID=3
    )
    
    println(s"Verification context created: isVerificationTask=${verificationContext.isVerificationTask}, targetElementId=${verificationContext.targetElementId}")
    
    // Set context
    TaskContext.setTaskContext(verificationContext)
    
    try {
      // Call iterator - should only return element at UID=3
      val iter = rdd.iterator(partition, verificationContext)
      val filtered = iter.toArray
      
      println(s"Verification mode result (should be single element): ${filtered.mkString(", ")}")
      println(s"Number of elements: ${filtered.length}")
      
      if (filtered.length == 1) {
        println("[SUCCESS] Single-element filtering works!")
        println(s"  Expected element at UID=3 is value 4 (input) → 8 (output)")
        println(s"  Got: ${filtered(0)}")
      } else {
        println(s"[FAILED] Expected 1 element, got ${filtered.length}")
      }
    } finally {
      TaskContext.unset()
    }
  }
}
