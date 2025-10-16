package org.apache.spark.examples

import java.io.File
import org.apache.spark.sql.SparkSession

object TestDroppers {
  def main(args: Array[String]): Unit = {
    println("=== Testing Category 1: 1→0/1 (Droppers) ===")
    
    val spark = SparkSession.builder()
      .appName("TestDroppers")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    try {
      // Clean up any existing log files
      cleanupLogFiles()
      
      testDroppers(sc)
      
      // Check log files after test
      println("\n=== Checking Generated Log Files ===")
      checkLogFiles()
      
    } finally {
      spark.stop()
    }
    
    println("\n=== Test Droppers Complete ===")
  }
  
  def testDroppers(sc: org.apache.spark.SparkContext): Unit = {
    println("Testing 1→0/1 transformations (should need UID queue management)")
    
    val input = sc.parallelize(1 to 10, 2)
    
    // Test 1: filter (already implemented)
    println("1. Testing filter() - should show UID queue logs")
    val filtered = input.filter(_ > 5)
    val filterResult = filtered.collect()
    println(s"Filter result: ${filterResult.mkString(", ")} (Expected: 6,7,8,9,10)")
    
    // Test 2: distinct (removes duplicates)
    println("2. Testing distinct() - should behave like filter")
    val inputWithDuplicates = sc.parallelize(List(1, 2, 2, 3, 3, 3, 4, 4, 5), 2)
    val distinctResult = inputWithDuplicates.distinct().collect().sorted
    println(s"Distinct result: ${distinctResult.mkString(", ")} (Expected: 1,2,3,4,5)")
    
    // Test 3: sample (withReplacement = false)
    println("3. Testing sample() - should behave like filter")
    val sampled = input.sample(withReplacement = false, fraction = 0.5, seed = 42)
    val sampleResult = sampled.collect()
    println(s"Sample result: ${sampleResult.mkString(", ")} (Expected: Variable due to randomness, ~5 elements)")
    
    // Test 4: Multiple filters (chain of droppers)
    println("4. Testing chained filters")
    val chainFiltered = input
      .filter(_ > 3)      // Expected: 4,5,6,7,8,9,10
      .filter(_ < 8)      // Expected: 4,5,6,7
      .filter(_ % 2 == 0) // Expected: 4,6
    val chainResult = chainFiltered.collect()
    println(s"Chain filter result: ${chainResult.mkString(", ")} (Expected: 4,6)")
    
    // Test 5: filter with no matches
    println("5. Testing filter with no matches")
    val emptyFilter = input.filter(_ > 20)
    val emptyResult = emptyFilter.collect()
    println(s"Empty filter result: ${emptyResult.mkString(", ")} (Expected: empty)")
  }
  
  def cleanupLogFiles(): Unit = {
    val currentDir = new File(".")
    val logFiles = currentDir.listFiles().filter { file =>
      file.getName.startsWith("spark_inputs_") || file.getName.startsWith("spark_finals_")
    }
    logFiles.foreach(_.delete())
    println(s"Cleaned up ${logFiles.length} existing log files")
  }
  
  def checkLogFiles(): Unit = {
    val userHome = System.getProperty("user.home")
    val currentDir = new File(s"$userHome/spark/spark-trace/TestDroppers")
    
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
