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
 * Test filter transformation for UID correlation.
 * 
 * Expected policy: REDUCED (1→0/1 mapping)
 * Some input elements are dropped.
 */
object TestFilter {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TestFilter")
      .master("local[2]")
      .getOrCreate()
    
    val sc = spark.sparkContext
    
    println("=== Testing filter Transformation ===")
    println("Transformation: filter(x => x > 5)")
    println("Expected policy: REDUCED (drops elements ≤ 5)")
    println()
    
    // Create simple dataset: [1,2,3,4,5] in partition 0, [6,7,8,9,10] in partition 1
    val data = sc.parallelize(1 to 10, 2)
    
    // Apply filter: keep only elements > 5
    val result = data.filter(x => x > 5)
    
    // Trigger execution
    val collected = result.collect()
    
    println(s"Input:  10 elements")
    println(s"Output: ${collected.length} elements (expected 5)")
    println(s"Results: ${collected.mkString(", ")}")
    println()
    println("=== Log files generated in ~/spark/spark-trace/TestFilter/logs ===")
    
    spark.stop()
  }
}
