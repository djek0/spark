package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object SimpleSparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleSparkTest")
      .getOrCreate()

    val sc = spark.sparkContext

    val numbers = sc.parallelize(1 to 10, 2)
    val doubled = numbers.map(_ * 2)
    doubled.collect().foreach(println)

    spark.stop()
  }
}


