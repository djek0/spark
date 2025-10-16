import org.apache.spark.sql.SparkSession

object TestLogging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test ResultTask Logging")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Create an RDD that will trigger ResultTask
    val rdd = sc.parallelize(1 to 10, 2)
    
    // This will trigger ResultTask.runTask()
    val result = rdd.collect()
    
    println(s"Result: ${result.mkString(", ")}")
    
    spark.stop()
  }
}
