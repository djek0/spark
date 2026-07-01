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
 * Minimal Spark job to generate test log files for parser validation.
 * 
 * This job creates a simple RDD, applies a map transformation, and collects results.
 * The UID tracking system will generate input and output log files.
 */
object GenerateTestLogs {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GenerateTestLogs")
      .master("local[2]")
      .getOrCreate()
    
    val sc = spark.sparkContext
    
    println("=== Generating Test Logs ===")
    println("Creating simple dataset with 2 partitions...")
    
    // Create simple dataset: [1,2,3,4,5] in partition 0, [6,7,8,9,10] in partition 1
    val data = sc.parallelize(1 to 10, 2)
    
    println("Applying map transformation (x => x * 2)...")
    
    // Apply simple map transformation
    val result = data.map(x => x * 2)
    
    println("Collecting results...")
    
    // Trigger execution
    val collected = result.collect()
    
    println(s"Results: ${collected.mkString(", ")}")
    println("\n=== Log files should be generated in /tmp ===")
    println("Look for files matching pattern: GenerateTestLogs_stage_*_partition_*_*.log")
    
    spark.stop()
  }
}
