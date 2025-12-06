package org.apache.spark.examples

import java.io.File
import org.apache.spark.sql.SparkSession

object TestExpanders {
  def main(args: Array[String]): Unit = {
    println("=== Testing Category 2: 1→M (Expanders) ===")
    
    val spark = SparkSession.builder()
      .appName("TestExpanders")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()
      
      testExpanders(sc)
      
      // Check log files after test
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()
      
    } finally {
      spark.stop()
    }
    
    println("\n=== Test Expanders Complete ===")
  }
  
  def testExpanders(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→M transformations (should need UID queue management)")
    
    val input = sc.parallelize(1 to 5, 2)
    
    // Test 1: flatMap
    println("1. Testing flatMap() - should show expansion behavior")
    val flatMapped = input.flatMap(x => List(x, x * 10))
    val flatMapResult = flatMapped.collect()
    println(s"FlatMap result: ${flatMapResult.mkString(", ")} (Expected: 1,10,2,20,3,30,4,40,5,50)")
    
    // Test 2: flatMapValues (on pair RDD)
    println("2. Testing flatMapValues() - should expand values")
    val pairRDD = input.map(x => (x % 3, x))
    val flatMapValues = pairRDD.flatMapValues(v => List(v, v * 2))
    val flatMapValuesResult = flatMapValues.collect()
    println(s"FlatMapValues result: ${flatMapValuesResult.mkString(", ")} (Expected: (1,1),(1,2),(2,2),(2,4),(0,3),(0,6),(1,4),(1,8),(2,5),(2,10))")
    
    // Test 3: flatMap with variable expansion
    println("3. Testing flatMap with variable expansion")
    val variableExpansion = input.flatMap(x => (1 to x).toList)
    val variableResult = variableExpansion.collect()
    println(s"Variable expansion result: ${variableResult.mkString(", ")} (Expected: 1,1,2,1,2,3,1,2,3,4,1,2,3,4,5)")
    
    // Test 4: flatMap with empty results
    println("4. Testing flatMap with some empty results")
    val mixedExpansion = input.flatMap(x => if (x % 2 == 0) List(x, x * 2) else List())
    val mixedResult = mixedExpansion.collect()
    println(s"Mixed expansion result: ${mixedResult.mkString(", ")} (Expected: 2,4,4,8)")
    
    // Test 5: flatMap with single element expansion
    println("5. Testing flatMap with single element expansion")
    val singleExpansion = input.flatMap(x => List(x * 100))
    val singleResult = singleExpansion.collect()
    println(s"Single expansion result: ${singleResult.mkString(", ")} (Expected: 100,200,300,400,500)")
  }
  
  def cleanupLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestExpanders")
    
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
    val currentDir = new File(s"$userHome/spark/spark-trace/TestExpanders")
    
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
