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

import org.apache.spark.{SparkConf, SparkContext}

object CollapserTest {
  
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: CollapserTest <test_type> <input_file> [partitions]")
      System.err.println("  test_type: coalesce|cartesian|zipPartitions|cogroup|subtract")
      System.exit(1)
    }
    
    val testType = args(0)
    val inputFile = args(1)
    val partitions = if (args.length > 2) args(2).toInt else 8
    
    val conf = new SparkConf().setAppName(s"Collapser Test - $testType")
    val sc = new SparkContext(conf)
    
    try {
      println("=" * 80)
      println(s"COLLAPSER TEST: $testType")
      println("=" * 80)
      println(s"Input File:  $inputFile")
      println(s"Partitions:  $partitions")
      println("=" * 80)
      println()
      
      testType.toLowerCase match {
        case "coalesce" => testCoalesce(sc, inputFile, partitions)
        case "cartesian" => testCartesian(sc, inputFile, partitions)
        case "zippartitions" => testZipPartitions(sc, inputFile, partitions)
        case "cogroup" => testCogroup(sc, inputFile, partitions)
        case "subtract" => testSubtract(sc, inputFile, partitions)
        case _ =>
          System.err.println(s"Unknown test type: $testType")
          System.exit(1)
      }
      
      println()
      println("=" * 80)
      println("TEST COMPLETED SUCCESSFULLY")
      println("=" * 80)
      
    } finally {
      sc.stop()
    }
  }
  
  def testCoalesce(sc: SparkContext, inputFile: String, partitions: Int): Unit = {
    println("[TEST] CoalescedRDD - repartition/coalesce")
    println("  This merges multiple partitions into fewer partitions")
    println()
    
    val data = sc.textFile(inputFile, minPartitions = partitions)
      .map(line => (line.hashCode % 100, line.take(20)))
    
    println(s"  Initial partitions: ${data.getNumPartitions}")
    
    val coalesced = data.coalesce(partitions / 2)
    println(s"  After coalesce: ${coalesced.getNumPartitions}")
    
    val count = coalesced.count()
    println(s"  Total records: $count")
    
    val sample = coalesced.take(5)
    println(s"  Sample: ${sample.mkString(", ")}")
  }
  
  def testCartesian(sc: SparkContext, inputFile: String, partitions: Int): Unit = {
    println("[TEST] CartesianRDD - cartesian product")
    println("  This creates all pairs from two RDDs")
    println()
    
    val data = sc.textFile(inputFile, minPartitions = partitions)
      .map(_.hashCode % 10)
      .distinct()
    
    val keys = sc.parallelize(1 to 5, partitions)
    
    println(s"  Data partitions: ${data.getNumPartitions}")
    println(s"  Keys partitions: ${keys.getNumPartitions}")
    
    val cartesian = data.cartesian(keys)
    println(s"  Cartesian partitions: ${cartesian.getNumPartitions}")
    
    val count = cartesian.count()
    println(s"  Total pairs: $count")
    
    val sample = cartesian.take(10)
    println(s"  Sample pairs: ${sample.mkString(", ")}")
  }
  
  def testZipPartitions(sc: SparkContext, inputFile: String, partitions: Int): Unit = {
    println("[TEST] ZippedPartitionsRDD - zipPartitions")
    println("  This zips corresponding partitions from multiple RDDs")
    println()
    
    val data1 = sc.textFile(inputFile, minPartitions = partitions)
      .map(line => (line.hashCode % 100, 1))
    
    val data2 = sc.textFile(inputFile, minPartitions = partitions)
      .map(line => (line.hashCode % 100, 2))
    
    println(s"  Data1 partitions: ${data1.getNumPartitions}")
    println(s"  Data2 partitions: ${data2.getNumPartitions}")
    
    val zipped = data1.zipPartitions(data2) { (iter1, iter2) =>
      iter1.zip(iter2).map { case ((k1, v1), (k2, v2)) => (k1, v1 + v2) }
    }
    
    println(s"  Zipped partitions: ${zipped.getNumPartitions}")
    
    val count = zipped.count()
    println(s"  Total records: $count")
    
    val sample = zipped.take(5)
    println(s"  Sample: ${sample.mkString(", ")}")
  }
  
  def testCogroup(sc: SparkContext, inputFile: String, partitions: Int): Unit = {
    println("[TEST] CoGroupedRDD - cogroup")
    println("  This groups values from multiple RDDs by key")
    println()
    
    val data1 = sc.textFile(inputFile, minPartitions = partitions)
      .map(line => (line.hashCode % 10, s"A:${line.take(10)}"))
    
    val data2 = sc.textFile(inputFile, minPartitions = partitions)
      .map(line => (line.hashCode % 10, s"B:${line.take(10)}"))
    
    println(s"  Data1 partitions: ${data1.getNumPartitions}")
    println(s"  Data2 partitions: ${data2.getNumPartitions}")
    
    val cogrouped = data1.cogroup(data2)
    println(s"  Cogrouped partitions: ${cogrouped.getNumPartitions}")
    
    val count = cogrouped.count()
    println(s"  Total groups: $count")
    
    val sample = cogrouped.take(3).map { case (k, (v1, v2)) =>
      s"($k, [${v1.size} from A, ${v2.size} from B])"
    }
    println(s"  Sample groups: ${sample.mkString(", ")}")
  }
  
  def testSubtract(sc: SparkContext, inputFile: String, partitions: Int): Unit = {
    println("[TEST] SubtractedRDD - subtract")
    println("  This removes elements in RDD2 from RDD1")
    println()
    
    val data1 = sc.textFile(inputFile, minPartitions = partitions)
      .map(_.hashCode % 100)
    
    val data2 = sc.textFile(inputFile, minPartitions = partitions)
      .map(_.hashCode % 100)
      .filter(_ % 2 == 0)
    
    println(s"  Data1 partitions: ${data1.getNumPartitions}")
    println(s"  Data2 partitions: ${data2.getNumPartitions}")
    
    val count1 = data1.count()
    val count2 = data2.count()
    println(s"  Data1 count: $count1")
    println(s"  Data2 count: $count2")
    
    val subtracted = data1.subtract(data2)
    println(s"  Subtracted partitions: ${subtracted.getNumPartitions}")
    
    val count = subtracted.count()
    println(s"  Result count: $count")
    
    val sample = subtracted.take(10)
    println(s"  Sample: ${sample.mkString(", ")}")
  }
}
