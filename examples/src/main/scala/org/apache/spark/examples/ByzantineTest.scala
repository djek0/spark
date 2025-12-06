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
 * Deterministic Byzantine Fault Tolerance Test
 * Computes sum of numbers - fully deterministic, no randomness
 * Perfect for testing Byzantine fault detection
 */
object ByzantineTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Byzantine Fault Detection Test")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices

    println(s"Running Byzantine test with $slices partitions, $n total elements")

    // Create RDD with deterministic computation: sum numbers 1 to n
    val sum = spark.sparkContext.parallelize(1 to n, slices)
      .map { i => 
        // Deterministic computation - same input always gives same output
        i * 2
      }
      .reduce(_ + _)

    // Expected result: sum of (1*2 + 2*2 + 3*2 + ... + n*2) = 2 * (n*(n+1)/2)
    val expected = n.toLong * (n + 1)
    
    println(s"Computed sum: $sum")
    println(s"Expected sum: $expected")
    
    if (sum == expected) {
      println("✓ Result is CORRECT - Byzantine detection passed or no faults injected")
    } else {
      println("✗ Result is WRONG - Byzantine fault may have corrupted result")
    }

    spark.stop()
  }
}
