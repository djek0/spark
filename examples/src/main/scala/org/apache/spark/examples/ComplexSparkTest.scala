package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object ComplexSparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ComplexSparkTest")
      .getOrCreate()

    val sc = spark.sparkContext

    // Stage 1: Parallelize input (root RDD)
    val input = sc.parallelize(1 to 10, 2)

    // Map (narrow transformation)
    val mapped = input.map(x => (x % 2, x * 3)) // Narrow transformation

    // Filter (still narrow)
    val filtered = mapped.filter { case (_, v) => v % 2 == 0 }

    // Stage 2: groupByKey (wide dependency, triggers a shuffle)
    val grouped = filtered.groupByKey()

    // Add a map and a filter after groupByKey
    val processed = grouped
      .map { case (key, values) =>
        val sum = values.sum
        (key, sum)
      }
      .filter { case (_, sum) => sum > 50 } // keep only groups with sum < 100

    // Final action: collect and print
    val result = processed.collect()

    println("== Final Output ==")
    result.foreach { case (key, sum) =>
      println(s"Group $key: sum = $sum")
    }

    spark.stop()
  }
}
