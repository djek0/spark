package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object Filter_Key_Value {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ComplexSparkTest")
      .getOrCreate()

    val sc = spark.sparkContext

    // Stage 1: Parallelize input (root RDD)
    val input = sc.parallelize(1 to 10, 2)

    // Map (narrow transformation)
    val mapped = input.map(x => (x , x * 2))// Narrow transformation

    // Filter (still narrow)
    val filtered = mapped.filter { case (_, v) => v > 10 }

    // Stage 2: groupByKey (wide dependency, triggers a shuffle)
    val grouped = filtered.groupByKey()

    // Final action: collect and print
    val result = grouped.collect()

    println("== Final Output ==")
    result.foreach { case (key, sum) =>
      println(s"($key, $sum)")
    }

    spark.stop()
  }
}
