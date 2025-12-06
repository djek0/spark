package org.apache.spark.examples

import java.io.File
import org.apache.spark.sql.SparkSession

object TestSafeTransformations {
  def main(args: Array[String]): Unit = {
    println("=== Testing Category 3: 1→1 (Safe Pass-through) ===")
    
    val spark = SparkSession.builder()
      .appName("TestSafeTransformations")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()
      
      testSafeTransformations(sc)
      
      // Check log files after test
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()
      
    } finally {
      spark.stop()
    }
    
    println("\n=== Test Safe Transformations Complete ===")
  }
  
  def testSafeTransformations(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→1 transformations (should NOT need UID queue management)")
    
    val input = sc.parallelize(1 to 5, 2)
    
    // Test 1: map
    println("1. Testing map() - should be pass-through")
    val mapped = input.map(_ * 2)
    val mapResult = mapped.collect()
    println(s"Map result: ${mapResult.mkString(", ")} (Expected: 2,4,6,8,10)")
    
    // Test 2: mapValues (on pair RDD)
    println("2. Testing mapValues() - should be pass-through")
    val pairRDD = input.map(x => (x % 3, x))
    val mapValues = pairRDD.mapValues(_ * 3)
    val mapValuesResult = mapValues.collect()
    println(s"MapValues result: ${mapValuesResult.mkString(", ")} (Expected: (1,3),(2,6),(0,9),(1,12),(2,15))")
    
    // Test 3: keyBy
    println("3. Testing keyBy() - should be pass-through")
    val keyBy = input.keyBy(_ % 2)
    val keyByResult = keyBy.collect()
    println(s"KeyBy result: ${keyByResult.mkString(", ")} (Expected: (1,1),(0,2),(1,3),(0,4),(1,5))")
    
    // Test 4: values and keys
    println("4. Testing values() and keys() - should be pass-through")
    val values = keyBy.values
    val keys = keyBy.keys
    val valuesResult = values.collect()
    val keysResult = keys.collect()
    println(s"Values result: ${valuesResult.mkString(", ")} (Expected: 1,2,3,4,5)")
    println(s"Keys result: ${keysResult.mkString(", ")} (Expected: 1,0,1,0,1)")
    
    // Test 5: mapPartitions (1→1 per partition)
    println("5. Testing mapPartitions() - should be pass-through")
    val mapPartitions = input.mapPartitions(iter => iter.map(_ + 100))
    val mapPartitionsResult = mapPartitions.collect()
    println(s"MapPartitions result: ${mapPartitionsResult.mkString(", ")} (Expected: 101,102,103,104,105)")
    
    // Test 6: mapPartitionsWithIndex (1→1 per partition)
    println("6. Testing mapPartitionsWithIndex() - should be pass-through")
    val mapPartitionsWithIndex = input.mapPartitionsWithIndex((idx, iter) => iter.map(_ + idx * 10))
    val mapPartitionsWithIndexResult = mapPartitionsWithIndex.collect()
    println(s"MapPartitionsWithIndex result: ${mapPartitionsWithIndexResult.mkString(", ")} (Expected: 1,2,13,14,15)")
  }
  
  def cleanupLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestSafeTransformations")
    
    // Create directory if it doesn't exist
    if (!currentDir.exists()) {
      currentDir.mkdirs()
    }
    
    val allFiles = Option(currentDir.listFiles()).getOrElse(Array.empty[File])
    val logFiles = allFiles.filter { file =>
      file.getName.startsWith("spark_inputs_") || file.getName.startsWith("spark_finals_")
    }
    logFiles.foreach(_.delete())
    println(s"Cleaned up ${logFiles.length} existing log files")
  }
  
  def checkLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestSafeTransformations")
    
    // Create directory if it doesn't exist
    if (!currentDir.exists()) {
      currentDir.mkdirs()
    }
    
    val allFiles = Option(currentDir.listFiles()).getOrElse(Array.empty[File])
    val inputLogs = allFiles.filter(_.getName.startsWith("spark_inputs_"))
    val outputLogs = allFiles.filter(_.getName.startsWith("spark_finals_"))

    println(s"Found ${inputLogs.length} input log files:")
    inputLogs.foreach(file => println(s"  - ${file.getName}"))

    println(s"Found ${outputLogs.length} output log files:")
    outputLogs.foreach(file => println(s"  - ${file.getName}"))

    // Group by stage and print: Input of stage N, then Output of stage N
    val StagePattern = "spark_(inputs|finals)_stage(\\d+)_p(\\d+)_att(\\d+)_taskId(\\d+)\\.(log|tmp)".r
    case class LogKey(kind: String, stage: Int, part: Int, att: Int, task: Long, name: String)

    def parseKey(f: File): Option[LogKey] = f.getName match {
      case StagePattern(kind, s, p, att, task, _) =>
        Some(LogKey(kind, s.toInt, p.toInt, att.toInt, task.toLong, f.getName))
      case _ => None
    }

    val allLogs = (Option(inputLogs).getOrElse(Array.empty) ++ Option(outputLogs).getOrElse(Array.empty))
    val keyed = allLogs.flatMap(f => parseKey(f).map(k => k -> f))
    val byStage = keyed.groupBy(_._1.stage).toSeq.sortBy(_._1)

    byStage.foreach { case (stage, entries) =>
      val inputs = entries.collect { case (k, f) if k.kind == "inputs" => (k, f) }
        .sortBy { case (k, _) => (k.part, k.task, k.att, k.name) }
      val finals = entries.collect { case (k, f) if k.kind == "finals" => (k, f) }
        .sortBy { case (k, _) => (k.part, k.task, k.att, k.name) }

      println(s"\n===== Stage $stage =====")

      println(s"Input of stage $stage:")
      if (inputs.isEmpty) println("  (no input logs found for this stage)")
      inputs.foreach { case (k, file) =>
        println(s"  - ${file.getName} (p=${k.part}, att=${k.att}, task=${k.task})")
        var src: Option[scala.io.BufferedSource] = None
        try {
          src = Some(scala.io.Source.fromFile(file))
          src.get.getLines().foreach(line => println(s"    $line"))
        } catch {
          case e: Exception => println(s"    Error reading file: ${e.getMessage}")
        } finally {
          try src.foreach(_.close()) catch { case _: Throwable => () }
        }
      }

      println(s"Output of stage $stage:")
      if (finals.isEmpty) println("  (no output logs found for this stage)")
      finals.foreach { case (k, file) =>
        println(s"  - ${file.getName} (p=${k.part}, att=${k.att}, task=${k.task})")
        var src: Option[scala.io.BufferedSource] = None
        try {
          src = Some(scala.io.Source.fromFile(file))
          src.get.getLines().foreach(line => println(s"    $line"))
        } catch {
          case e: Exception => println(s"    Error reading file: ${e.getMessage}")
        } finally {
          try src.foreach(_.close()) catch { case _: Throwable => () }
        }
      }
    }
  }
}
