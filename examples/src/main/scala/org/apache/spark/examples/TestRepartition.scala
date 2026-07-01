/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

/**
 * Test repartition transformation for UID correlation.
 * 
 * Expected policy: PRESERVED + ORDER_INDEPENDENT
 * All UIDs preserved but order may change due to repartitioning.
 */
object TestRepartition {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TestRepartition")
      .master("local[2]")
      .getOrCreate()
    
    val sc = spark.sparkContext
    
    println("=== Testing repartition Transformation ===")
    println("Transformation: repartition(3)")
    println("Expected policy: PRESERVED + ORDER_INDEPENDENT")
    println("(UIDs preserved but order may change)")
    println()
    
    // Create dataset with 2 partitions: [1,2,3,4,5] and [6,7,8,9,10]
    val data = sc.parallelize(1 to 10, 2)
    
    // Repartition to 3 partitions - this will shuffle data
    val result = data.repartition(3)
    
    // Trigger execution
    val collected = result.collect()
    
    println(s"Input:  10 elements in 2 partitions")
    println(s"Output: ${collected.length} elements in 3 partitions")
    println(s"Results: ${collected.mkString(", ")}")
    println()
    println("=== Log files generated in ~/spark/spark-trace/TestRepartition/logs ===")
    
    spark.stop()
  }
}
