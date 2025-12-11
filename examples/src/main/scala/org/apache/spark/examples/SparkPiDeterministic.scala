// scalastyle:off println
package org.apache.spark.examples

import scala.util.Random

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi with DETERMINISTIC results for Byzantine fault tolerance */
object SparkPiDeterministic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi Deterministic")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      // CRITICAL: Use seeded Random for deterministic results across replicas
      // Each value of 'i' gets the same seed, so replicas compute identical results
      val rng = new Random(i.toLong)
      val x = rng.nextDouble() * 2 - 1
      val y = rng.nextDouble() * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}
// scalastyle:on println
