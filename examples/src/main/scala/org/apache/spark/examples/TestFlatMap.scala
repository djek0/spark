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
 * Test flatMap transformation for UID correlation.
 * 
 * Expected policy: EXPANDED (1→N mapping)
 * Each input element produces multiple output elements.
 */
object TestFlatMap {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TestFlatMap")
      .master("local[2]")
      .getOrCreate()
    
    val sc = spark.sparkContext
    
    println("=== Testing flatMap Transformation ===")
    println("Transformation: flatMap(x => List(x, x*2, x*3))")
    println("Expected policy: EXPANDED (1→3 mapping)")
    println()
    
    // Create simple dataset: [1,2,3,4,5] in partition 0, [6,7,8,9,10] in partition 1
    val data = sc.parallelize(1 to 10, 2)
    
    // Apply flatMap: each element produces 3 outputs
    val result = data.flatMap(x => List(x, x * 2, x * 3))
    
    // Trigger execution
    val collected = result.collect()
    
    println(s"Input:  10 elements")
    println(s"Output: ${collected.length} elements (expected 30)")
    println(s"Results: ${collected.mkString(", ")}")
    println()
    println("=== Log files generated in ~/spark/spark-trace/TestFlatMap/logs ===")
    
    spark.stop()
  }
}
