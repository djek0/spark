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
 * Test sortByKey transformation for UID correlation.
 * 
 * Expected policy: PRESERVED + ORDER_INDEPENDENT
 * All UIDs preserved but order changes due to sorting.
 */
object TestSortByKey {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TestSortByKey")
      .master("local[2]")
      .getOrCreate()
    
    val sc = spark.sparkContext
    
    println("=== Testing sortByKey Transformation ===")
    println("Transformation: map(x => (x, x*10)).sortByKey(ascending=false)")
    println("Expected policy: PRESERVED + ORDER_INDEPENDENT")
    println("(UIDs preserved but order reversed)")
    println()
    
    // Create key-value pairs in ascending order: (1,10), (2,20), ..., (10,100)
    val data = sc.parallelize(1 to 10, 2)
      .map(x => (x, x * 10))
    
    // Sort by key in descending order - this will reverse the order
    val result = data.sortByKey(ascending = false)
    
    // Trigger execution
    val collected = result.collect()
    
    println(s"Input:  10 key-value pairs (ascending order)")
    println(s"Output: ${collected.length} key-value pairs (descending order)")
    println(s"Results: ${collected.mkString(", ")}")
    println()
    println("=== Log files generated in ~/spark/spark-trace/TestSortByKey/logs ===")
    
    spark.stop()
  }
}
