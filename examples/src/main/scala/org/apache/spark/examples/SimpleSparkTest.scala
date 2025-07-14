
// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SimpleSparkTest {
  def main(args: Array[String]): Unit = {
    println("=== Starting SimpleSparkTest ===")
    
    // 1. First print to verify basic execution
    println("1. Before creating SparkSession")
    
    // 2. Create SparkSession with more detailed configuration
    val spark = SparkSession.builder()
      .appName("SimpleSparkTest")
      .master("local[2]")  // Using 2 cores
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/home/djek0/spark/conf/log4j.properties")
      .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/home/djek0/spark/conf/log4j.properties")
      .getOrCreate()
      
    println("2. After creating SparkSession")

    val sc = spark.sparkContext
    
    // 3. Set log level programmatically
    sc.setLogLevel("INFO")
    println("3. After setting log level")
    
    // 4. Print Spark version and config
    println(s"Spark version: ${spark.version}")
    println(s"Spark master: ${spark.conf.get("spark.master")}")
    
    // 5. Create RDD with a print statement
    println("4. Creating RDD...")
    val numbers = sc.parallelize(1 to 10, 2)
    
    // 6. Add print statements in the map function
    val doubled = numbers.map { x => 
      println(s"[MAP] Processing number: $x")
      x * 2 
    }
    
    // 7. Collect and print results
    println("5. Collecting results...")
    val results = doubled.collect()
    println("6. Results:")
    results.foreach(println)
    
    // 8. Add a log message using Spark's logging
    println("7. Adding log message through Spark's logging")
    sc.parallelize(Seq(1)).foreach { _ => 
      println("[EXECUTOR] This is a message from the executor")
    }
    
    // 9. Stop Spark
    println("8. Stopping Spark...")
    spark.stop()
    println("=== SimpleSparkTest completed ===")
  }
}
// scalestayle:on println
